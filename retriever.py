"""
retriever.py — Subflow OmniLake Hybrid Retriever & Lightning Synthesis Engine

Implements the sub-second multi-domain retrieval and reasoning pipeline:
  1. Dense Query Embedding (jpro-router gpt-embedding, 1024-d Matryoshka Guard)
  2. LanceDB Hybrid Search (Vector + BM25 FTS + DuckDB prefilter)
  3. 1-Hop Causal Graph Expansion (from causal_graphs/graph_edges.parquet)
  4. Cross-Encoder Precision Reranking (jpro-router gpt-rerank)
  5. Nemotron Lightning Synthesis (jpro-router gpt-6-luna) with YouTube Evidence Cards
"""

import argparse
import json
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

import duckdb
import lancedb
import requests

from domains import extract_domain_entities, get_domain_cartridge
from indexer import normalize_l2
from pipeline import ROUTER_HOST, resolve_router_client


class OmniLakeRetriever:
    """
    Subflow OmniLake Hybrid Retriever & Reasoning Engine.
    Executes sub-second hybrid retrieval across LanceDB, DuckDB Parquet causal triples,
    jpro-router cross-encoder reranking, and Lightning LLM synthesis.
    """

    def __init__(
        self,
        db_dir: str = "./data/omnilake/lancedb",
        parquet_path: str = "./data/omnilake/causal_graphs/graph_edges.parquet",
    ):
        self.db_dir = Path(db_dir)
        self.parquet_path = Path(parquet_path)
        self.db = lancedb.connect(str(self.db_dir))
        self.table_name = "knowledge_chunks"
        self.table = self.db.open_table(self.table_name)

        self.router_host = ROUTER_HOST
        self.auth_token = self._resolve_auth_token()

    def _resolve_auth_token(self) -> str:
        """Fetches router auth token from /connect or env."""
        try:
            res = requests.get(f"{self.router_host}/connect", timeout=3)
            if res.status_code == 200:
                return res.json().get("auth_token", "local-proxy-token")
        except Exception:
            pass
        return os.environ.get("LOCAL_PROXY_AUTH_TOKEN", "local-proxy-token")

    def embed_query(self, query: str) -> List[float]:
        """Generates 1024-d L2-normalized vector for query via jpro-router."""
        url = f"{self.router_host}/openai/v1/embeddings"
        headers = {
            "Authorization": f"Bearer {self.auth_token}",
            "Content-Type": "application/json",
        }
        payload = {
            "model": "gpt-embedding",
            "input": [query],
            "dimensions": 1024,
        }
        res = requests.post(url, json=payload, headers=headers, timeout=20)
        res.raise_for_status()
        raw_emb = res.json()["data"][0]["embedding"]
        return normalize_l2(raw_emb, target_dim=1024)

    def rerank(
        self, query: str, documents: List[str]
    ) -> List[Dict[str, Any]]:
        """Invokes jpro-router POST /openai/v1/rerank (model='gpt-rerank')."""
        if not documents:
            return []
        url = f"{self.router_host}/openai/v1/rerank"
        headers = {
            "Authorization": f"Bearer {self.auth_token}",
            "Content-Type": "application/json",
        }
        payload = {
            "model": "gpt-rerank",
            "query": query,
            "documents": documents,
        }
        try:
            res = requests.post(url, json=payload, headers=headers, timeout=30)
            res.raise_for_status()
            data = res.json()
            return data.get("results", [])
        except Exception as e:
            print(f"[Rerank Warning] Fallback due to reranker error: {e}")
            return [{"index": i, "relevance_score": 0.5} for i in range(len(documents))]

    def get_causal_triples_for_entities(
        self, entities: List[str], domain: Optional[str] = None, limit: int = 10
    ) -> List[Dict[str, Any]]:
        """Queries graph_edges.parquet via DuckDB for 1-hop causal relationships."""
        if not entities or not self.parquet_path.exists():
            return []

        con = duckdb.connect()
        try:
            entity_literals = ", ".join(f"'{e.lower()}'" for e in entities)
            where_domain = f"AND domain = '{domain}'" if domain else ""
            query = f"""
                SELECT subject, predicate, object, evidence_text, citation_url, authority_score
                FROM '{self.parquet_path}'
                WHERE (LOWER(subject) IN ({entity_literals}) OR LOWER(object) IN ({entity_literals}))
                {where_domain}
                ORDER BY authority_score DESC
                LIMIT {limit}
            """
            rows = con.execute(query).fetchall()
            return [
                {
                    "subject": r[0],
                    "predicate": r[1],
                    "object": r[2],
                    "evidence_text": r[3],
                    "citation_url": r[4],
                    "authority_score": r[5],
                }
                for r in rows
            ]
        except Exception:
            return []

    def search(
        self,
        query: str,
        domain: Optional[str] = None,
        sub_domain: Optional[str] = None,
        min_authority: float = 0.50,
        top_candidates: int = 30,
        top_k: int = 6,
    ) -> List[Dict[str, Any]]:
        """
        Executes multi-stage hybrid search:
          1. Dense vector search with SQL prefilter
          2. BM25 keyword search with SQL prefilter
          3. Reciprocal Rank Fusion (RRF) with authority score & tier weighting
          4. Cross-encoder precision rerank (gpt-rerank)
          5. 1-hop Causal Graph enrichment
        """
        # Build SQL prefilter
        filters = ["sponsorship_audited = true", f"authority_score >= {min_authority}"]
        if domain:
            filters.append(f"domain = '{domain}'")
        if sub_domain:
            filters.append(f"sub_domain = '{sub_domain}'")
        where_clause = " AND ".join(filters)

        # 1. Dense vector search
        query_vec = self.embed_query(query)
        dense_results = (
            self.table.search(query_vec)
            .where(where_clause, prefilter=True)
            .limit(top_candidates)
            .to_list()
        )

        # 2. BM25 FTS search
        fts_results: List[Dict[str, Any]] = []
        try:
            # Strip non-alphanumeric special characters for FTS
            clean_q = re.sub(r"[^\w\s]", " ", query).strip()
            if clean_q:
                fts_results = (
                    self.table.search(clean_q, query_type="fts")
                    .where(where_clause, prefilter=True)
                    .limit(top_candidates)
                    .to_list()
                )
        except Exception:
            pass

        # 3. Reciprocal Rank Fusion (RRF)
        dense_rank = {r["chunk_id"]: rank for rank, r in enumerate(dense_results)}
        fts_rank = {r["chunk_id"]: rank for rank, r in enumerate(fts_results)}

        all_candidate_map = {}
        for r in dense_results:
            all_candidate_map[r["chunk_id"]] = r
        for r in fts_results:
            all_candidate_map[r["chunk_id"]] = r

        rrf_scored = []
        for cid, cand in all_candidate_map.items():
            r_d = dense_rank.get(cid, 999)
            r_f = fts_rank.get(cid, 999)
            auth = cand.get("authority_score", 0.6)
            tier_mult = 1.15 if cand.get("chunk_tier") == "distilled_claim" else 1.00

            rrf_score = tier_mult * (1.0 + 0.2 * auth) * (
                (1.0 / (60.0 + r_d)) + (1.0 / (60.0 + r_f))
            )
            rrf_scored.append((rrf_score, cand))

        rrf_scored.sort(key=lambda x: x[0], reverse=True)
        top_pool = [c for _, c in rrf_scored[:top_candidates]]

        if not top_pool:
            return []

        # 4. Cross-Encoder Precision Reranking via jpro-router (gpt-rerank)
        doc_texts = [c["clean_text"] for c in top_pool]
        rerank_results = self.rerank(query, doc_texts)

        final_candidates = []
        for res in rerank_results:
            idx = res.get("index", 0)
            rel_score = float(res.get("relevance_score", 0.5))
            if idx < len(top_pool):
                cand = top_pool[idx]
                auth = cand.get("authority_score", 0.6)
                final_score = 0.75 * rel_score + 0.25 * auth
                cand["relevance_score"] = rel_score
                cand["final_score"] = final_score
                final_candidates.append(cand)

        final_candidates.sort(key=lambda x: x.get("final_score", 0.0), reverse=True)
        top_k_chunks = final_candidates[:top_k]

        # 5. 1-hop Causal Graph Enrichment
        collected_entities: List[str] = []
        for c in top_k_chunks:
            collected_entities.extend(c.get("entities", []))
        collected_entities = sorted(set(collected_entities))

        causal_triples = self.get_causal_triples_for_entities(
            entities=collected_entities, domain=domain, limit=8
        )

        for c in top_k_chunks:
            c["matched_causal_triples"] = [
                t for t in causal_triples
                if t["subject"] in c.get("entities", []) or t["object"] in c.get("entities", [])
            ]

        return top_k_chunks

    def ask(
        self,
        query: str,
        domain: Optional[str] = None,
        sub_domain: Optional[str] = None,
        top_k: int = 6,
    ) -> Dict[str, Any]:
        """
        Executes full reasoning pipeline:
          Search -> Ground Truth Context assembly -> Nemotron Lightning LLM synthesis -> Evidence Cards
        """
        top_chunks = self.search(
            query=query,
            domain=domain,
            sub_domain=sub_domain,
            top_k=top_k,
        )

        if not top_chunks:
            return {
                "answer": "해당 질문과 일치하는 지식 청크를 데이터베이스에서 찾지 못했습니다.",
                "evidence_cards": [],
                "causal_triples": [],
            }

        # Build Context Prompt
        context_blocks = []
        for idx, c in enumerate(top_chunks, start=1):
            tier_label = "Verified Claim" if c["chunk_tier"] == "distilled_claim" else "Transcript"
            context_blocks.append(
                f"[Source #{idx} | {c['title']} | {c['channel_or_journal']} | {tier_label} | {c['citation_url']}]\n"
                f"{c['raw_text']}"
            )

        context_str = "\n\n".join(context_blocks)

        all_triples = []
        for c in top_chunks:
            all_triples.extend(c.get("matched_causal_triples", []))

        triples_str = ""
        if all_triples:
            triples_lines = [
                f"- ({t['subject']}) --[{t['predicate']}]--> ({t['object']}) [Source: {t['citation_url']}]"
                for t in all_triples[:6]
            ]
            triples_str = "\n=== Verified Causal Mechanisms ===\n" + "\n".join(triples_lines) + "\n"

        system_instruction = (
            "You are an Elite Knowledge Synthesis Specialist powered by Subflow OmniLake.\n"
            "Synthesize an accurate, high-density, evidence-grounded answer to the user's question.\n"
            "RULES:\n"
            "1. Base your answer strictly on the provided Ground Truth Context and Causal Mechanisms.\n"
            "2. Whenever stating a claim, metric, or protocol, append the exact source citation markdown link, "
            "e.g., `([Huberman Lab 42:15](https://youtu.be/...))`.\n"
            "3. Answer in the same language as the user's query (Korean for Korean queries, English for English queries).\n"
            "4. Provide a structured, clear response with bolded key terms and bulleted steps."
        )

        user_content = (
            f"=== Ground Truth Knowledge Sources ===\n"
            f"{context_str}\n"
            f"{triples_str}\n"
            f"=== User Question ===\n"
            f"{query}"
        )

        # Call jpro-router using standard OpenAI client from resolve_router_client()
        client, slot_model = resolve_router_client()
        target_model = slot_model if slot_model else "gpt-6.1-sol"

        resp = client.chat.completions.create(
            model=target_model,
            messages=[
                {"role": "system", "content": system_instruction},
                {"role": "user", "content": user_content},
            ],
            temperature=0.15,
            max_tokens=4096,
            timeout=180.0,
        )

        choice = resp.choices[0]
        answer = choice.message.content or getattr(choice.message, "reasoning", "") or "답변 생성 완료."

        # Build Evidence Cards
        evidence_cards = []
        for c in top_chunks:
            start_sec = c["timestamp_start"]
            m, s = int(start_sec // 60), int(start_sec % 60)
            evidence_cards.append({
                "chunk_id": c["chunk_id"],
                "domain": c["domain"],
                "sub_domain": c["sub_domain"],
                "channel": c["channel_or_journal"],
                "title": c["title"],
                "timestamp": f"{m:02d}:{s:02d}",
                "citation_url": c["citation_url"],
                "authority_score": c["authority_score"],
                "relevance_score": c.get("relevance_score", 0.0),
                "entities": c.get("entities", []),
                "snippet": c["raw_text"][:220] + "..." if len(c["raw_text"]) > 220 else c["raw_text"],
            })

        return {
            "answer": answer,
            "evidence_cards": evidence_cards,
            "causal_triples": all_triples,
        }


def main():
    parser = argparse.ArgumentParser(description="Subflow OmniLake Hybrid Retriever & Reasoner CLI")
    parser.add_argument("query", type=str, help="Question to ask OmniLake (KR or EN)")
    parser.add_argument("--domain", type=str, default=None, choices=["medical", "seo", "fermentation"], help="Target domain filter")
    parser.add_argument("--sub-domain", type=str, default=None, help="Specific sub-domain filter")
    parser.add_argument("--top-k", type=int, default=5, help="Number of evidence chunks to retrieve")
    parser.add_argument("--search-only", action="store_true", help="Print retrieved chunks only without LLM synthesis")

    args = parser.parse_args()

    retriever = OmniLakeRetriever()

    if args.search_only:
        print(f"\n🔍 [OmniLake Hybrid Search] Query: '{args.query}' (Domain: {args.domain or 'all'})")
        results = retriever.search(query=args.query, domain=args.domain, sub_domain=args.sub_domain, top_k=args.top_k)
        for i, r in enumerate(results, 1):
            print(f"\n[{i}] {r['title']} ({r['channel_or_journal']}) - Score: {r.get('final_score', 0):.3f}")
            print(f"    Link: {r['citation_url']}")
            print(f"    Tier: {r['chunk_tier']} | Entities: {r.get('entities', [])}")
            print(f"    Text: {r['raw_text'][:200]}...")
    else:
        print(f"\n🧠 [OmniLake Reasoning Engine] '{args.query}'...")
        res = retriever.ask(query=args.query, domain=args.domain, sub_domain=args.sub_domain, top_k=args.top_k)
        print("\n" + "=" * 60)
        print("💡 [OmniLake Synthesis Answer]")
        print("=" * 60)
        print(res["answer"])
        print("\n" + "=" * 60)
        print("📌 [Ground Truth Evidence Cards]")
        print("=" * 60)
        for i, card in enumerate(res["evidence_cards"], 1):
            print(f"{i}. [{card['channel']}] {card['title']} [{card['timestamp']}]")
            print(f"   URL: {card['citation_url']} (Authority: {card['authority_score']:.2f})")
            print(f"   Snippet: {card['snippet']}\n")


if __name__ == "__main__":
    main()

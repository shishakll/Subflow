"""
indexer.py — Subflow OmniLake Indexer

Implements the 4-Stage Ingestion & Dual-Track Chunking Pipeline:
  Stage 1: Triple-Shield Sponsor & Noise Auditor (SponsorBlock + VTT Rolling Dedup + SPONSOR_REGEX)
  Stage 2A: Track A Distilled Claims & Causal Triples from structured_notes/*.md
  Stage 2B: Track B Verbatim Semantic Windows from cleaned transcripts
  Stage 3: Contextual Header Injection & Canonical Entity Tagging
  Stage 4: jpro-router Embedding (1024-d Matryoshka Guard) & LanceDB + DuckDB/Parquet Upsert
"""

import hashlib
import json
import math
import os
import re
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import duckdb
import lancedb
import pyarrow as pa
import pyarrow.parquet as pq
import requests

from domains import (
    SPONSOR_REGEX,
    calculate_authority_score,
    extract_domain_entities,
    get_domain_cartridge,
)
from pipeline import (
    ROUTER_HOST,
    extract_non_overlapping_delta,
    fetch_sponsorblock,
    parse_time,
    resolve_router_client,
)

# ── 1. Unified Arrow Schemas ──────────────────────────────────────────────────
KNOWLEDGE_CHUNKS_SCHEMA = pa.schema([
    pa.field("chunk_id", pa.string()),
    pa.field("domain", pa.string()),
    pa.field("sub_domain", pa.string()),
    pa.field("chunk_tier", pa.string()),
    pa.field("source_type", pa.string()),
    pa.field("source_id", pa.string()),
    pa.field("channel_or_journal", pa.string()),
    pa.field("title", pa.string()),
    pa.field("timestamp_start", pa.float32()),
    pa.field("timestamp_end", pa.float32()),
    pa.field("citation_url", pa.string()),
    pa.field("context_header", pa.string()),
    pa.field("clean_text", pa.string()),
    pa.field("raw_text", pa.string()),
    pa.field("vector", pa.list_(pa.float32(), 1024)),
    pa.field("entities", pa.list_(pa.string())),
    pa.field("authority_score", pa.float32()),
    pa.field("sponsorship_audited", pa.bool_()),
    pa.field("domain_meta", pa.string()),
    pa.field("file_hash", pa.string()),
    pa.field("created_at", pa.float64()),
])

GRAPH_EDGES_SCHEMA = pa.schema([
    pa.field("edge_id", pa.string()),
    pa.field("domain", pa.string()),
    pa.field("sub_domain", pa.string()),
    pa.field("subject", pa.string()),
    pa.field("predicate", pa.string()),
    pa.field("object", pa.string()),
    pa.field("evidence_text", pa.string()),
    pa.field("source_id", pa.string()),
    pa.field("timestamp_start", pa.float32()),
    pa.field("citation_url", pa.string()),
    pa.field("authority_score", pa.float32()),
    pa.field("chunk_id", pa.string()),
    pa.field("created_at", pa.float64()),
])

# ── 2. Vector Math: 1024-d Matryoshka Slicing & L2 Normalization ──────────────
def normalize_l2(vector: List[float], target_dim: int = 1024) -> List[float]:
    """
    Applies Matryoshka prefix slicing (if > 1024-d) and unit L2 normalization:
    v_norm = v[:target_dim] / sqrt(sum(v_i^2) + 1e-12)
    """
    if len(vector) < target_dim:
        raise ValueError(
            f"Vector dimension ({len(vector)}) is smaller than required target_dim ({target_dim})"
        )
    sliced = vector[:target_dim]
    norm = math.sqrt(sum(x * x for x in sliced) + 1e-12)
    return [float(x / norm) for x in sliced]


# ── 3. Stage 1: Triple-Shield Sponsor & Noise Auditor ─────────────────────────
def parse_and_clean_segments(
    file_path: Path, video_id: str
) -> List[Dict[str, Any]]:
    """
    Parses VTT/SRT files with 3-tier noise defense:
      1. SponsorBlock crowd-sourced interval cuts
      2. Rolling-caption suffix-prefix overlap elimination
      3. Bilingual SPONSOR_REGEX window slicing
    """
    raw = file_path.read_text(encoding="utf-8", errors="ignore")
    sb_intervals = fetch_sponsorblock(video_id)

    pattern = re.compile(
        r"(\d{1,2}:\d{2}(?::\d{2})?[.,]\d{1,3})\s*-->\s*(\d{1,2}:\d{2}(?::\d{2})?[.,]\d{1,3}).*?\n(.*?)(?=\n\n|\n\d+\n|\Z)",
        re.DOTALL,
    )
    matches = pattern.findall(raw)

    cleaned_segments: List[Dict[str, Any]] = []
    prev_text = ""
    regex_skip_until = 0.0

    for start_str, end_str, content in matches:
        start_sec = parse_time(start_str)
        end_sec = parse_time(end_str)

        # Shield 1: SponsorBlock API intervals
        if any(s <= start_sec <= e for s, e in sb_intervals):
            continue

        # Shield 3: Regex skip window active
        if start_sec < regex_skip_until:
            continue

        # Strip HTML tags, bracketed sounds [Music], (laughter)
        line = re.sub(r"<[^>]+>", "", content).replace("\n", " ").strip()
        line = re.sub(r"\[.*?\]|\(.*?\)", "", line)
        line = re.sub(r"\s+", " ", line).strip()
        if not line or line == prev_text:
            continue

        # Shield 3: Check for commercial sponsor trigger
        if SPONSOR_REGEX.search(line):
            # Skip from trigger for 60 seconds
            regex_skip_until = start_sec + 60.0
            continue

        # Shield 2: Rolling caption suffix-prefix deduplication
        delta = extract_non_overlapping_delta(prev_text, line)
        prev_text = line
        if not delta:
            continue

        # Strip common spoken filler words
        delta = re.sub(r"\b(um|uh|you know|어|음|그니까)\b", "", delta, flags=re.IGNORECASE).strip()
        if not delta:
            continue

        cleaned_segments.append({
            "start": float(start_sec),
            "end": float(max(end_sec, start_sec + 1.0)),
            "text": delta,
        })

    return cleaned_segments


# ── 4. Stage 2B: Track B Verbatim Window Chunker ──────────────────────────────
def chunk_verbatim_windows(
    segments: List[Dict[str, Any]],
    video_id: str,
    title: str,
    channel: str,
    domain: str = "medical",
    sub_domain: str = "general",
    file_hash: str = "",
    target_words: int = 180,
    overlap_words: int = 35,
) -> List[Dict[str, Any]]:
    """
    Groups de-duplicated spoken segments into sentence-bounded 150-250 word windows
    with contextual metadata headers.
    """
    chunks: List[Dict[str, Any]] = []
    current_words: List[str] = []
    chunk_start = 0.0
    chunk_index = 0

    for seg in segments:
        text = seg["text"].strip()
        if not text:
            continue

        if not current_words:
            chunk_start = seg["start"]

        words = text.split(" ")
        current_words.extend(words)

        if len(current_words) >= target_words:
            chunk_end = seg["end"]
            raw_body = " ".join(current_words).strip()

            start_m, start_s = int(chunk_start // 60), int(chunk_start % 60)
            end_m, end_s = int(chunk_end // 60), int(chunk_end % 60)
            header = f"[Domain: {domain}/{sub_domain}] [Channel: {channel}] [Title: {title}] [Time: {start_m}m{start_s}s-{end_m}m{end_s}s]"
            clean_text = f"{header} {raw_body}"

            entities = extract_domain_entities(clean_text, domain=domain)
            authority = calculate_authority_score(
                domain=domain,
                channel_or_journal=channel,
                source_type="youtube",
                is_distilled=False,
            )

            chunk_id = f"{domain}_{video_id}_v{chunk_index:04d}"
            citation_url = f"https://youtu.be/{video_id}?t={int(chunk_start)}"

            domain_meta = json.dumps({
                "window_words": len(current_words),
                "entities_count": len(entities),
            })

            chunks.append({
                "chunk_id": chunk_id,
                "domain": domain,
                "sub_domain": sub_domain,
                "chunk_tier": "verbatim_window",
                "source_type": "youtube",
                "source_id": video_id,
                "channel_or_journal": channel,
                "title": title,
                "timestamp_start": float(chunk_start),
                "timestamp_end": float(chunk_end),
                "citation_url": citation_url,
                "context_header": header,
                "clean_text": clean_text,
                "raw_text": raw_body,
                "entities": entities,
                "authority_score": float(authority),
                "sponsorship_audited": True,
                "domain_meta": domain_meta,
                "file_hash": file_hash,
                "created_at": float(time.time()),
            })

            chunk_index += 1
            current_words = current_words[-overlap_words:]

    # Handle remaining tail segments
    if len(current_words) >= 40 and segments:
        chunk_end = segments[-1]["end"]
        raw_body = " ".join(current_words).strip()
        start_m, start_s = int(chunk_start // 60), int(chunk_start % 60)
        end_m, end_s = int(chunk_end // 60), int(chunk_end % 60)
        header = f"[Domain: {domain}/{sub_domain}] [Channel: {channel}] [Title: {title}] [Time: {start_m}m{start_s}s-{end_m}m{end_s}s]"
        clean_text = f"{header} {raw_body}"
        entities = extract_domain_entities(clean_text, domain=domain)
        authority = calculate_authority_score(domain, channel, "youtube", is_distilled=False)

        chunks.append({
            "chunk_id": f"{domain}_{video_id}_v{chunk_index:04d}",
            "domain": domain,
            "sub_domain": sub_domain,
            "chunk_tier": "verbatim_window",
            "source_type": "youtube",
            "source_id": video_id,
            "channel_or_journal": channel,
            "title": title,
            "timestamp_start": float(chunk_start),
            "timestamp_end": float(chunk_end),
            "citation_url": f"https://youtu.be/{video_id}?t={int(chunk_start)}",
            "context_header": header,
            "clean_text": clean_text,
            "raw_text": raw_body,
            "entities": entities,
            "authority_score": float(authority),
            "sponsorship_audited": True,
            "domain_meta": json.dumps({"window_words": len(current_words)}),
            "file_hash": file_hash,
            "created_at": float(time.time()),
        })

    return chunks


# ── 5. Stage 2A: Track A Distilled Claims & Causal Triples ────────────────────
CAUSAL_VERB_REGEX = re.compile(
    r"\b([A-Z0-9][A-Za-z0-9_\-\s]{1,30})\s+"
    r"(activates?|inhibits?|upregulates?|downregulates?|requires?|produces?|prevents?|increases?|decreases?|triggers?|induces?)\s+"
    r"([A-Z0-9][A-Za-z0-9_\-\s]{1,35})\b",
    re.IGNORECASE,
)

PREDICATE_NORMALIZATION = {
    "activate": "ACTIVATES", "activates": "ACTIVATES",
    "inhibit": "INHIBITS", "inhibits": "INHIBITS",
    "upregulate": "UPREGULATES", "upregulates": "UPREGULATES",
    "downregulate": "DOWNREGULATES", "downregulates": "DOWNREGULATES",
    "require": "REQUIRES", "requires": "REQUIRES",
    "produce": "PRODUCES", "produces": "PRODUCES",
    "prevent": "PREVENTS", "prevents": "PREVENTS",
    "increase": "INCREASES", "increases": "INCREASES",
    "decrease": "DECREASES", "decreases": "DECREASES",
    "trigger": "ACTIVATES", "triggers": "ACTIVATES",
    "induce": "ACTIVATES", "induces": "ACTIVATES",
}


def parse_timestamp_from_text(text: str) -> Optional[float]:
    """Extracts first [MM:SS] or [HH:MM:SS] citation in seconds."""
    match = re.search(r"\[(\d{1,3}:\d{2}(?::\d{2})?)\]", text)
    if not match:
        return None
    raw_ts = match.group(1)
    parts = [float(p) for p in raw_ts.split(":")]
    if len(parts) == 2:
        return parts[0] * 60.0 + parts[1]
    elif len(parts) == 3:
        return parts[0] * 3600.0 + parts[1] * 60.0 + parts[2]
    return None


def extract_distilled_claims_and_triples(
    markdown_path: Path,
    video_id: str,
    title: str,
    channel: str,
    domain: str = "medical",
    sub_domain: str = "general",
    file_hash: str = "",
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Parses structured_notes/*.md into:
      1. Track A Atomic Chunks (top-level claim + sub-bullet mechanisms)
      2. Causal Triples (Subject -> Predicate -> Object) for the causal lakehouse
    """
    content = markdown_path.read_text(encoding="utf-8", errors="ignore")
    lines = content.split("\n")

    chunks: List[Dict[str, Any]] = []
    triples: List[Dict[str, Any]] = []

    current_section = ""
    current_claim_title = ""
    current_bullet_lines: List[str] = []
    claim_index = 0

    def finalize_claim():
        nonlocal claim_index, current_bullet_lines, current_claim_title
        if not current_bullet_lines:
            return

        full_claim_text = "\n".join(current_bullet_lines).strip()
        ts_start = parse_timestamp_from_text(full_claim_text) or 0.0
        ts_end = ts_start + 60.0

        start_m, start_s = int(ts_start // 60), int(ts_start % 60)
        header = f"[Domain: {domain}/{sub_domain}] [Channel: {channel}] [Section: {current_section}] [Time: {start_m}m{start_s}s]"
        clean_text = f"{header}\n{full_claim_text}"

        entities = extract_domain_entities(clean_text, domain=domain)
        authority = calculate_authority_score(
            domain=domain,
            channel_or_journal=channel,
            source_type="youtube",
            is_distilled=True,
            has_empirical_data=("### 3." in current_section or bool(re.search(r"\d+%", full_claim_text))),
        )

        chunk_id = f"{domain}_{video_id}_c{claim_index:04d}"
        citation_url = f"https://youtu.be/{video_id}?t={int(ts_start)}"

        domain_meta = json.dumps({
            "section": current_section,
            "claim_title": current_claim_title,
            "sub_bullets_count": len(current_bullet_lines) - 1,
        })

        chunks.append({
            "chunk_id": chunk_id,
            "domain": domain,
            "sub_domain": sub_domain,
            "chunk_tier": "distilled_claim",
            "source_type": "youtube",
            "source_id": video_id,
            "channel_or_journal": channel,
            "title": title,
            "timestamp_start": float(ts_start),
            "timestamp_end": float(ts_end),
            "citation_url": citation_url,
            "context_header": header,
            "clean_text": clean_text,
            "raw_text": full_claim_text,
            "entities": entities,
            "authority_score": float(authority),
            "sponsorship_audited": True,
            "domain_meta": domain_meta,
            "file_hash": file_hash,
            "created_at": float(time.time()),
        })

        # Extract Causal Triples from claim text
        for line in current_bullet_lines:
            for match in CAUSAL_VERB_REGEX.finditer(line):
                subj, raw_pred, obj = match.groups()
                subj = subj.strip()
                obj = obj.strip()
                norm_pred = PREDICATE_NORMALIZATION.get(raw_pred.lower(), raw_pred.upper())

                # Filter out overly common or trivial matches
                if len(subj) < 2 or len(obj) < 2:
                    continue
                if subj.lower() in ("this", "it", "that", "there", "which", "he", "she"):
                    continue

                edge_key = f"{domain}_{video_id}_{subj}_{norm_pred}_{obj}_{int(ts_start)}"
                edge_id = hashlib.sha1(edge_key.encode("utf-8")).hexdigest()[:16]

                triples.append({
                    "edge_id": edge_id,
                    "domain": domain,
                    "sub_domain": sub_domain,
                    "subject": subj,
                    "predicate": norm_pred,
                    "object": obj,
                    "evidence_text": line.strip(),
                    "source_id": video_id,
                    "timestamp_start": float(ts_start),
                    "citation_url": citation_url,
                    "authority_score": float(authority),
                    "chunk_id": chunk_id,
                    "created_at": float(time.time()),
                })

        claim_index += 1
        current_bullet_lines = []
        current_claim_title = ""

    for line in lines:
        stripped = line.strip()
        if stripped.startswith("### "):
            finalize_claim()
            current_section = stripped
            continue

        # Top-level bullet: - **Title**: ...
        if stripped.startswith("- ") and not stripped.startswith("-  "):
            finalize_claim()
            current_bullet_lines.append(stripped)
            match_title = re.search(r"-\s+\*\*(.*?)\*\*", stripped)
            if match_title:
                current_claim_title = match_title.group(1)
        elif stripped.startswith("  - ") or stripped.startswith("    - "):
            if current_bullet_lines:
                current_bullet_lines.append(stripped)

    finalize_claim()
    return chunks, triples


# ── 6. OmniLake Indexer Class ─────────────────────────────────────────────────
class OmniLakeIndexer:
    """
    Subflow OmniLake Indexer & Lakehouse Store.
    Connects to jpro-router (http://127.0.0.1:4001/openai/v1) for embeddings and
    persists into LanceDB (knowledge_chunks.lance) and Parquet/DuckDB (graph_edges.parquet).
    """

    def __init__(
        self,
        db_dir: str = "./data/omnilake/lancedb",
        parquet_path: str = "./data/omnilake/causal_graphs/graph_edges.parquet",
    ):
        self.db_dir = Path(db_dir)
        self.db_dir.mkdir(parents=True, exist_ok=True)
        self.parquet_path = Path(parquet_path)
        self.parquet_path.parent.mkdir(parents=True, exist_ok=True)

        self.db = lancedb.connect(str(self.db_dir))
        self.table_name = "knowledge_chunks"
        self.table = self._init_table()

        # Resolve router client details
        _, self.slot_model = resolve_router_client()
        self.router_host = ROUTER_HOST
        self.auth_token = self._resolve_auth_token()

    def _resolve_auth_token(self) -> str:
        """Fetches auth token from router /connect endpoint or env."""
        try:
            res = requests.get(f"{self.router_host}/connect", timeout=3)
            if res.status_code == 200:
                return res.json().get("auth_token", "local-proxy-token")
        except Exception:
            pass
        return os.environ.get("LOCAL_PROXY_AUTH_TOKEN", "local-proxy-token")

    def _init_table(self):
        """Initializes or opens knowledge_chunks LanceDB table."""
        if self.table_name not in self.db.table_names():
            return self.db.create_table(self.table_name, schema=KNOWLEDGE_CHUNKS_SCHEMA)
        return self.db.open_table(self.table_name)

    def embed_batch(
        self, texts: List[str], batch_size: int = 16
    ) -> List[List[float]]:
        """
        Invokes jpro-router POST /openai/v1/embeddings (model="gpt-embedding", dimensions=1024)
        with Matryoshka slicing and L2 unit-norm normalization.
        """
        url = f"{self.router_host}/openai/v1/embeddings"
        headers = {
            "Authorization": f"Bearer {self.auth_token}",
            "Content-Type": "application/json",
        }

        all_embeddings: List[List[float]] = []

        for i in range(0, len(texts), batch_size):
            batch = texts[i : i + batch_size]
            payload = {
                "model": "gpt-embedding",
                "input": batch,
                "dimensions": 1024,
            }
            res = requests.post(url, json=payload, headers=headers, timeout=60)
            res.raise_for_status()
            data = res.json().get("data", [])

            for item in data:
                raw_emb = item["embedding"]
                norm_emb = normalize_l2(raw_emb, target_dim=1024)
                all_embeddings.append(norm_emb)

        return all_embeddings

    def upsert_chunks(self, chunks: List[Dict[str, Any]], batch_size: int = 16):
        """Generates vectors and upserts chunks into LanceDB."""
        if not chunks:
            return

        # Check existing chunk_ids BEFORE embedding to avoid redundant API calls
        existing_ids = set()
        try:
            df = self.table.search().limit(1).to_pandas()
            if not df.empty:
                tbl_arrow = self.table.to_arrow()
                con = duckdb.connect()
                res = con.execute("SELECT chunk_id FROM tbl_arrow").fetchall()
                existing_ids = {r[0] for r in res}
        except Exception:
            pass

        new_chunks = [c for c in chunks if c["chunk_id"] not in existing_ids]
        if not new_chunks:
            print(f"[OmniLake] ℹ️ {len(chunks)}개 청크가 이미 색인되어 있어 임베딩 생성을 건너뜁니다.")
            return

        print(f"[OmniLake] {len(new_chunks)}개 신규 청크 임베딩 생성 시작 (jpro-router gpt-embedding)...")
        texts_to_embed = [c["clean_text"] for c in new_chunks]
        embeddings = self.embed_batch(texts_to_embed, batch_size=batch_size)

        for chunk, emb in zip(new_chunks, embeddings):
            chunk["vector"] = emb

        self.table.add(new_chunks)
        print(f"[OmniLake] ✅ {len(new_chunks)}개 신규 청크 LanceDB 적재 완료 (중복 제외 {len(chunks)-len(new_chunks)}개).")

        # Ensure FTS index
        try:
            self.table.create_fts_index("clean_text", replace=True)
            print("[OmniLake] ✅ LanceDB BM25 FTS 인덱스 갱신 완료.")
        except Exception as e:
            pass

    def upsert_causal_triples(self, triples: List[Dict[str, Any]]):
        """Appends/upserts causal triples into graph_edges.parquet."""
        if not triples:
            return

        con = duckdb.connect()
        new_table = pa.Table.from_pylist(triples, schema=GRAPH_EDGES_SCHEMA)

        if self.parquet_path.exists():
            # Merge with existing edges de-duplicating by edge_id
            existing_table = pq.read_table(str(self.parquet_path))
            combined = con.sql("""
                SELECT * FROM (
                    SELECT *, ROW_NUMBER() OVER (PARTITION BY edge_id ORDER BY created_at DESC) as rn
                    FROM (
                        SELECT * FROM existing_table
                        UNION ALL
                        SELECT * FROM new_table
                    )
                ) WHERE rn = 1
            """).to_arrow_table()
            pq.write_table(combined, str(self.parquet_path))
            print(f"[OmniLake] ✅ Causal Graph Parquet 업데이트: 총 {len(combined)}개 인과 엣지 보관 중.")
        else:
            pq.write_table(new_table, str(self.parquet_path))
            print(f"[OmniLake] ✅ Causal Graph Parquet 생성: {len(new_table)}개 인과 엣지 저장 완료.")

    def index_video(
        self,
        video_id: str,
        title: str,
        channel: str,
        domain: str = "medical",
        sub_domain: str = "general",
        vtt_path: Optional[Path] = None,
        md_path: Optional[Path] = None,
        mode: str = "deep",
    ) -> Dict[str, int]:
        """
        Coordinates full indexing of a video:
          - Track B (Verbatim Windows from VTT)
          - Track A (Distilled Claims & Triples from Markdown notes if mode == 'deep')
        """
        print(f"\n============================================================")
        print(f"📦 OmniLake 인덱싱 시작: [{domain}/{sub_domain}] {title} ({video_id})")
        print(f"============================================================")

        file_hash = ""
        all_chunks: List[Dict[str, Any]] = []
        all_triples: List[Dict[str, Any]] = []

        # 1. Track B: Verbatim Windows
        if vtt_path and vtt_path.exists():
            file_bytes = vtt_path.read_bytes()
            file_hash = hashlib.sha256(file_bytes).hexdigest()
            print(f"[Stage 1] VTT 3중 노이즈 정제 및 세그먼트 파싱 중 ({vtt_path.name})...")
            segments = parse_and_clean_segments(vtt_path, video_id)
            print(f"[Stage 2B] Track B (Verbatim Windows) 생성 중 (정제된 발화 세그먼트: {len(segments)}개)...")
            verbatim_chunks = chunk_verbatim_windows(
                segments=segments,
                video_id=video_id,
                title=title,
                channel=channel,
                domain=domain,
                sub_domain=sub_domain,
                file_hash=file_hash,
            )
            all_chunks.extend(verbatim_chunks)
            print(f"  -> Track B 청크 생성 완료: {len(verbatim_chunks)}개")

        # 2. Track A: Distilled Claims & Causal Triples
        if mode == "deep" and md_path and md_path.exists():
            print(f"[Stage 2A] Track A (Distilled Claims & Causal Triples) 추출 중 ({md_path.name})...")
            distilled_chunks, triples = extract_distilled_claims_and_triples(
                markdown_path=md_path,
                video_id=video_id,
                title=title,
                channel=channel,
                domain=domain,
                sub_domain=sub_domain,
                file_hash=file_hash,
            )
            all_chunks.extend(distilled_chunks)
            all_triples.extend(triples)
            print(f"  -> Track A 청크: {len(distilled_chunks)}개 | 인과 트리플: {len(triples)}개")

        # 3. Upsert to LanceDB and Parquet
        self.upsert_chunks(all_chunks)
        self.upsert_causal_triples(all_triples)

        return {
            "total_chunks": len(all_chunks),
            "verbatim_chunks": len([c for c in all_chunks if c["chunk_tier"] == "verbatim_window"]),
            "distilled_chunks": len([c for c in all_chunks if c["chunk_tier"] == "distilled_claim"]),
            "causal_triples": len(all_triples),
        }

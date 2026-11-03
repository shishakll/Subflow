# Subflow OmniLake Map

Label: `wayfinder:map`

## Destination

Deliver the complete Technical Specification ([`spec.md`](./spec.md)), Domain Glossary ([`GLOSSARY.md`](../../GLOSSARY.md)), and modular implementation roadmap for **Subflow MitoLake** dedicated exclusively to medical longevity (mitochondrial health, anti-aging, brain health, antioxidant defense, and skin aging prevention) backed by LanceDB, DuckDB, KuzuDB, and `jpro-router`'s OpenAI virtual sockets (`gpt-embedding`, `gpt-rerank`, `gpt-6.1-sol`, `gpt-6-luna`).

## Notes

- **Domain**: Dedicated medical longevity knowledge lakehouse & causal retrieval pipeline (`medical` / `MitoLake`).
- **Skills to consult**: `domain-modeling`, `codebase-design`, `tdd`.
- **Standing invariants**:
  1. All LLM, Embedding, and Rerank calls route strictly through local `jpro-router` (`http://127.0.0.1:4001/openai/v1/*`) at `$0.00` cost.
  2. Embedding socket `gpt-embedding` (`/openai/v1/embeddings`) is called with `dimensions=1024` + L2-normalized Matryoshka dimension guard.
  3. Rerank socket `gpt-rerank` (`/openai/v1/rerank`) re-scores hybrid candidates before `gpt-6-luna` Lightning synthesis.
  4. Single dedicated LanceDB table (`knowledge_chunks.lance`) with `domain="medical"`, `sub_domain`, `chunk_tier`, `entities`, `authority_score`, and DuckDB-queryable `domain_meta` JSON.

## Decisions so far

- [Verify and wire jpro-router OpenAI Embedding and Rerank sockets](./issues/01-jpro-router-embedding-and-rerank-sockets.md): Added `/openai/v1/embeddings` (`gpt-embedding`, 1024-d MRL) and `/openai/v1/rerank` (`gpt-rerank`) passthrough routes with key rotation and model fallback to `jpro-router`, and verified live with `gpt-6-luna` (`nvidia/nemotron-3.5-lightning:free`).
- [Lock Dedicated MitoLake Schema and Exclude Other Domains](./issues/02-multidomain-lakehouse-schema-and-cartridges.md): Dedicated the lakehouse exclusively to `medical` (`MitoLake`), completely eliminating all references and data for SEO (`RankLake`) and fermentation (`ZymoLake`) to prevent any domain confusion.
- [Lock Dual-Track Chunking and Triple-Shield Sponsor Defense](./issues/03-dual-track-chunking-and-triple-shield-auditor.md): Combined SponsorBlock intervals, VTT rolling-caption suffix-prefix deduplication, and bilingual `SPONSOR_REGEX` slicing across Track B (`verbatim_window`) and Track A (`distilled_claim` + `Causal Triple` extraction from `structured_notes/*.md`).
- [Implement Domain Cartridge Registry and OmniLake Indexer](./issues/04-implement-domains-and-omnilake-indexer.md): Created `domains.py` and `indexer.py` with medical longevity ontology, Triple-Shield noise defense, Dual-Track chunking, 1024-d Matryoshka L2 embeddings via `jpro-router` (`gpt-embedding`), and LanceDB + DuckDB Parquet upsert.
- [Implement Hybrid Retriever and End-to-End CLI Integration](./issues/05-implement-hybrid-retriever-and-cli-integration.md): Created `retriever.py` with dense + BM25 RRF + 1-hop causal expansion + `gpt-rerank` + `gpt-6-luna` Lightning synthesis with timestamp evidence cards, and extended `collector.py` and `pipeline.py` with `--domain`, `--sub-domain`, `--channel`, and `--index` CLI flags.

## Not yet specified

- None — All 5 Wayfinder milestones are fully resolved, implemented, and verified live!

## Out of scope

- Fermentation (`ZymoLake`) and SEO (`RankLake`) — completely removed from the codebase and lakehouse per user instruction to avoid any confusion.
- Remote paid API integrations (OpenAI, Anthropic, or Cohere direct billing endpoints) — violates the `$0.00` local `jpro-router` invariant.
- Server-based vector databases requiring Docker/JVM daemons (Milvus, Qdrant server, Weaviate) — violates the 8 GB Apple Silicon M1 memory constraint.

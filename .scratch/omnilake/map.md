# Subflow OmniLake Map

Label: `wayfinder:map`

## Destination

Deliver the complete Technical Specification ([`spec.md`](./spec.md)), Domain Glossary ([`GLOSSARY.md`](../../GLOSSARY.md)), and modular implementation roadmap for **Subflow OmniLake** (`MitoLake` medical longevity + `RankLake` SEO + `ZymoLake` fermentation recipes) backed by LanceDB, DuckDB, KuzuDB, and `jpro-router`'s OpenAI virtual sockets (`gpt-embedding`, `gpt-rerank`, `gpt-6.1-sol`, `gpt-6-luna`).

## Notes

- **Domain**: Multi-domain multimedia knowledge lakehouse & causal retrieval pipeline (`medical`, `seo`, `fermentation`).
- **Skills to consult**: `domain-modeling`, `codebase-design`, `tdd`.
- **Standing invariants**:
  1. All LLM, Embedding, and Rerank calls route strictly through local `jpro-router` (`http://127.0.0.1:4001/openai/v1/*`) at `$0.00` cost.
  2. Embedding socket `gpt-embedding` (`/openai/v1/embeddings`) is called with `dimensions=1024` + L2-normalized Matryoshka dimension guard.
  3. Rerank socket `gpt-rerank` (`/openai/v1/rerank`) re-scores hybrid candidates before `gpt-6-luna` Lightning synthesis.
  4. Single unified LanceDB table (`knowledge_chunks.lance`) with `domain`, `sub_domain`, `chunk_tier`, `entities`, `authority_score`, and DuckDB-queryable `domain_meta` JSON.

## Decisions so far

- [Verify and wire jpro-router OpenAI Embedding and Rerank sockets](./issues/01-jpro-router-embedding-and-rerank-sockets.md): Added `/openai/v1/embeddings` (`gpt-embedding`, 1024-d MRL) and `/openai/v1/rerank` (`gpt-rerank`) passthrough routes with key rotation and model fallback to `jpro-router`, and verified live with `gpt-6-luna` (`nvidia/nemotron-3.5-lightning:free`).
- [Lock Multi-Domain Lakehouse Schema and Domain Cartridges](./issues/02-multidomain-lakehouse-schema-and-cartridges.md): Adopted a single unified `knowledge_chunks.lance` Arrow schema (`domain`, `sub_domain`, `chunk_tier`, `entities`, `authority_score`, `domain_meta` JSON) plus `graph_edges.parquet`/KuzuDB causal triples, and recorded canonical terms in `GLOSSARY.md` and `ADR-0001`.
- [Lock Dual-Track Chunking and Triple-Shield Sponsor Defense](./issues/03-dual-track-chunking-and-triple-shield-auditor.md): Combined SponsorBlock intervals, VTT rolling-caption suffix-prefix deduplication, and bilingual `SPONSOR_REGEX` slicing across Track B (`verbatim_window`) and Track A (`distilled_claim` + `Causal Triple` extraction from `structured_notes/*.md`).

## Not yet specified

- None — all architectural decisions for [`spec.md`](./spec.md) are resolved and ready for implementation tickets ([`04-implement-domains-and-omnilake-indexer.md`](./issues/04-implement-domains-and-omnilake-indexer.md) and [`05-implement-hybrid-retriever-and-cli-integration.md`](./issues/05-implement-hybrid-retriever-and-cli-integration.md)).

## Out of scope

- Remote paid API integrations (OpenAI, Anthropic, or Cohere direct billing endpoints) — violates the `$0.00` local `jpro-router` invariant.
- Server-based vector databases requiring Docker/JVM daemons (Milvus, Qdrant server, Weaviate) — violates the 8 GB Apple Silicon M1 memory constraint.

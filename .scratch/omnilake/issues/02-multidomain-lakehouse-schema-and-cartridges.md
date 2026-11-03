# 02: Lock Multi-Domain Lakehouse Schema and Domain Cartridges

Type: grilling
Status: resolved
Blocked by: 01

## Question

How should the LanceDB + DuckDB + KuzuDB schema and domain terminology be structured so that Medical Longevity (`MitoLake`) works immediately while SEO (`RankLake`) and Fermentation Recipes (`ZymoLake`) are supported globally without schema migrations?

## Answer

1. **Unified Table + Polymorphic `domain_meta`**: Use a single `knowledge_chunks.lance` table in `data/omnilake/lancedb/` with `domain` (`medical` | `seo` | `fermentation`), `sub_domain`, `chunk_tier` (`distilled_claim` | `verbatim_window`), `entities` (`list<string>`), `authority_score` (`float32`), `sponsorship_audited` (`bool`), and `domain_meta` (JSON string queryable via DuckDB).
2. **Causal Graph**: Store `(subject, predicate, object, timestamp_start, citation_url, authority_score, chunk_id)` in `data/omnilake/causal_graphs/graph_edges.parquet` and KuzuDB.
3. **Domain Glossary & ADR**: Canonicalized terms (`OmniLake`, `Domain Cartridge`, `MitoLake`, `RankLake`, `ZymoLake`, `Atomic Chunk`, `Distilled Claim`, `Verbatim Window`, `Authority Score`, `Causal Triple`, `Sponsor Contamination`) in [`GLOSSARY.md`](../../../GLOSSARY.md) and [`docs/adr/0001-omnilake-multidomain-lakehouse-and-jpro-router-sockets.md`](../../../docs/adr/0001-omnilake-multidomain-lakehouse-and-jpro-router-sockets.md).

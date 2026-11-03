# 04: Implement Domain Cartridge Registry (`domains.py`) and OmniLake Indexer (`indexer.py`)

Type: task
Status: ready-for-agent
Blocked by: 01, 02, 03

**What to build:** Declarative multi-domain cartridge registry (`domains.py`) for `medical` (`MitoLake`), `seo` (`RankLake`), and `fermentation` (`ZymoLake`), plus the `OmniLakeIndexer` (`indexer.py`) implementing Triple-Shield VTT cleaning, Dual-Track chunking (Track A Markdown claims/triples + Track B verbatim windows), `jpro-router` `gpt-embedding` (1024-d MRL + L2 normalization), and LanceDB + DuckDB/Parquet + KuzuDB upsert.

- [ ] Create `domains.py` with `DOMAIN_CARTRIDGES` (`medical`, `seo`, `fermentation`), bilingual `SPONSOR_REGEX`, canonical entity matchers, authority score calculators, and domain-specific structuring prompts.
- [ ] Implement `parse_and_clean_segments()` in `indexer.py` combining `fetch_sponsorblock()`, `extract_non_overlapping_delta()`, and `SPONSOR_REGEX` slicing.
- [ ] Implement `chunk_verbatim_windows()` (Track B) and `extract_distilled_claims_and_triples()` (Track A from `structured_notes/*.md`).
- [ ] Implement `OmniLakeIndexer` connecting to `jpro-router` (`GET /connect` -> `POST /openai/v1/embeddings` with `model="gpt-embedding"`, `dimensions=1024`), enforcing the 1024-dim L2-normalized vector invariant, and upserting into `data/omnilake/lancedb/knowledge_chunks.lance` (with BM25 FTS index) and `data/omnilake/causal_graphs/graph_edges.parquet`.

# 04: Implement Domain Cartridge Registry (`domains.py`) and OmniLake Indexer (`indexer.py`)

Type: task
Status: resolved
Blocked by: 01, 02, 03

**What to build:** Declarative multi-domain cartridge registry (`domains.py`) for `medical` (`MitoLake`), `seo` (`RankLake`), and `fermentation` (`ZymoLake`), plus the `OmniLakeIndexer` (`indexer.py`) implementing Triple-Shield VTT cleaning, Dual-Track chunking (Track A Markdown claims/triples + Track B verbatim windows), `jpro-router` `gpt-embedding` (1024-d MRL + L2 normalization), and LanceDB + DuckDB/Parquet + KuzuDB upsert.

- [x] Create `domains.py` with `DOMAIN_CARTRIDGES` (`medical`, `seo`, `fermentation`), bilingual `SPONSOR_REGEX`, canonical entity matchers, authority score calculators, and domain-specific structuring prompts.
- [x] Implement `parse_and_clean_segments()` in `indexer.py` combining `fetch_sponsorblock()`, `extract_non_overlapping_delta()`, and `SPONSOR_REGEX` slicing.
- [x] Implement `chunk_verbatim_windows()` (Track B) and `extract_distilled_claims_and_triples()` (Track A from `structured_notes/*.md`).
- [x] Implement `OmniLakeIndexer` connecting to `jpro-router` (`GET /connect` -> `POST /openai/v1/embeddings` with `model="gpt-embedding"`, `dimensions=1024`), enforcing the 1024-dim L2-normalized vector invariant, and upserting into `data/omnilake/lancedb/knowledge_chunks.lance` (with BM25 FTS index) and `data/omnilake/causal_graphs/graph_edges.parquet`.

## Answer

Implemented `domains.py` and `indexer.py` with full Multi-Domain support (`medical`, `seo`, `fermentation`), Triple-Shield noise defense (SponsorBlock + Suffix-Prefix overlap + `SPONSOR_REGEX`), Dual-Track indexing (Track A Distilled Claims from `structured_notes/*.md` + Track B Verbatim Windows from VTT), 1024-d Matryoshka L2-normalized embeddings via `jpro-router`'s `gpt-embedding`, and LanceDB + DuckDB Parquet persistence. Verified live by indexing `lZc30sUfx64` into 244 chunks and 1 causal triple.

# 05: Implement Hybrid Retriever (`retriever.py`) and End-to-End CLI Integration (`collector.py`, `pipeline.py`)

Type: task
Status: ready-for-agent
Blocked by: 04

**What to build:** Millisecond Hybrid Retrieval & Reasoning Engine (`retriever.py`) executing `gpt-embedding` -> LanceDB Dense + BM25 RRF + DuckDB prefilter + Causal Graph 1-hop expansion -> `gpt-rerank` -> `gpt-6-luna` (Nemotron Lightning) synthesis with clickable YouTube timestamp Evidence Cards, plus `--domain`, `--sub-domain`, and `--index` CLI flags in `collector.py` and `pipeline.py`.

- [ ] Implement `OmniLakeRetriever` in `retriever.py` with `search()` (Hybrid Dense + BM25 RRF + DuckDB `domain`/`authority_score` prefilter + 1-hop causal triple expansion) and `rerank()` (`POST /openai/v1/rerank`, `model="gpt-rerank"`).
- [ ] Implement `ask()` in `retriever.py` calling `POST /openai/v1/chat/completions` (`model="gpt-6-luna"`) to synthesize answers grounded in Top-K chunks with `https://youtu.be/{video_id}?t={sec}` Evidence Cards.
- [ ] Extend `collector.py` and `pipeline.py` with `--domain`, `--sub-domain`, `--channel`, and `--index` CLI flags and backward-compatible SQLite `jobs` table columns.
- [ ] Verify end-to-end ingestion and retrieval against existing transcripts/notes in `transcripts/` and `structured_notes/`.

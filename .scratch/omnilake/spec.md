# Subflow OmniLake: Technical Specification Document

**Document Version:** 3.0.0  
**Status:** Approved / Ready for Implementation  
**Target Environment:** macOS (Apple Silicon M1, 8 GB RAM) + Local `jpro-router` (`http://127.0.0.1:4001`)  
**Branch:** `feat/omnilake-pipeline`

---

## 1. Executive Summary & Mission

**Subflow OmniLake** extends Subflow's `$0.00` YouTube transcript collection and structuring pipeline into a **multi-domain Lakehouse and Causal Reasoning Engine** powered by **LanceDB (Apache Arrow)**, **DuckDB**, **KuzuDB**, and **`jpro-router` virtual model sockets**.

While the initial deployment targets **Medical Longevity (`MitoLake`)**—covering anti-aging, mitochondrial health, brain health, antioxidant defense, and skin aging prevention—the storage schema, chunking engine, and retrieval pipeline are architected globally to support **SEO & Content Growth (`RankLake`)**, **Fermentation Science & Recipes (`ZymoLake`)**, and future verticals without schema migrations.

### 1.1 Core Engineering Invariants
1. **100% Local `jpro-router` Routing ($0 Cost)**: Every LLM, Embedding, and Rerank call routes exclusively through the local `jpro-router` proxy (`http://127.0.0.1:4001/openai/v1/*`). Direct connections to remote `openrouter.ai` from Subflow are prohibited.
2. **8 GB Apple Silicon Memory Discipline**: No heavy Docker containers or resident JVM/Python model servers. Storage uses zero-copy disk-backed `mmap` via **LanceDB** (`knowledge_chunks.lance`), out-of-core vectorized SQL via **DuckDB**, and embedded disk graphs via **KuzuDB / Parquet** (<100 MB idle RAM).
3. **Triple-Shield Sponsor Contamination Defense**: Combines crowd-sourced **SponsorBlock API intervals**, **VTT rolling-caption suffix-prefix deduplication**, and **bilingual `SPONSOR_REGEX` window slicing** so commercial pitches (e.g., AG1, promo codes, affiliate links) never enter the vector space.
4. **Dual-Track Atomic Indexing**:
   - **Track A (`distilled_claim`)**: Zero-noise, LLM-verified hierarchical claims and `(Subject)-[Predicate]->(Object)` **Causal Triples** parsed from `structured_notes/*.md`.
   - **Track B (`verbatim_window`)**: Sentence-boundary-aligned 400–700 token sliding windows of de-duplicated transcripts with `[MM:SS]` anchors.
5. **Exact Timestamp Provenance**: Every retrieved **Atomic Chunk** preserves `source_id` (`video_id` or `PMID`), `timestamp_start`, `timestamp_end`, and a clickable deep link (`https://youtu.be/{video_id}?t={int(timestamp_start)}`).

---

## 2. `jpro-router` OpenAI Endpoint & Virtual Socket Contract

Subflow dynamically binds to `jpro-router` (`http://127.0.0.1:4001`) via `GET /connect` and communicates strictly through four OpenAI-compatible endpoints and virtual sockets:

### 2.1 Discovery Contract (`GET http://127.0.0.1:4001/connect`)
```json
{
  "auth_token": "local-proxy-token",
  "endpoints": {
    "openai": {
      "base_url": "http://127.0.0.1:4001/openai/v1",
      "chat_completions_url": "http://127.0.0.1:4001/openai/v1/chat/completions",
      "responses_url": "http://127.0.0.1:4001/openai/v1/responses",
      "embeddings_url": "http://127.0.0.1:4001/openai/v1/embeddings",
      "rerank_url": "http://127.0.0.1:4001/openai/v1/rerank",
      "models_url": "http://127.0.0.1:4001/openai/v1/models",
      "default_model": "gpt-6.1-sol",
      "available_slots": [
        "gpt-6.1-sol",
        "gpt-6-astra",
        "gpt-6.1-terra",
        "gpt-6-luna",
        "gpt-embedding",
        "gpt-rerank",
        "gpt-decision"
      ]
    }
  }
}
```

### 2.2 Virtual Socket & Endpoint Mapping Table

| Pipeline Stage | `jpro-router` OpenAI Endpoint | Virtual Socket (`model`) | Primary Upstream Model (Managed by `jpro-router`) | Request / Response Contract |
| :--- | :--- | :--- | :--- | :--- |
| **1. Deep Transcript Structuring & Judge** | `POST /openai/v1/chat/completions` | `gpt-6.1-sol` | `nvidia/nemotron-3-ultra-550b-a55b:free` (1M ctx) | Standard OpenAI Chat Completions (`temperature=0.15`, `max_tokens=16384`) |
| **2. Dense Vector Embedding** | `POST /openai/v1/embeddings` | `gpt-embedding` | `nvidia/nemotron-3-embed-1b:free` (32K ctx) | Request: `{"model": "gpt-embedding", "input": [...], "dimensions": 1024}`<br>Response: OpenAI `data[].embedding` (`1024` float32 values) |
| **3. Cross-Encoder Precision Reranking** | `POST /openai/v1/rerank` | `gpt-rerank` | `nvidia/llama-nemotron-rerank-vl-1b-v2:free` (10K ctx) | Request: `{"model": "gpt-rerank", "query": "...", "documents": [...], "top_n": 8}`<br>Response: `{"results": [{"index": i, "relevance_score": s, "document": {"text": "..."}}]}` |
| **4. Millisecond Reasoning (Lightning)** | `POST /openai/v1/chat/completions` | `gpt-6-luna` | `nvidia/nemotron-3.5-lightning:free` (1M ctx) | Standard OpenAI Chat Completions (`temperature=0.1`, streaming or non-streaming) |

### 2.3 Embedding Dimension Guard (1024-Dim Invariant)
Because `nvidia/nemotron-3-embed-1b:free` outputs `2048` dimensions by default when `dimensions` is omitted and `1024` dimensions via Matryoshka Representation Learning (MRL) when `"dimensions": 1024` is specified—and because `gpt-embedding` has fallback models in `jpro-router`—Subflow enforces a deterministic **Dimension Guard** before LanceDB insertion:
1. Always pass `"dimensions": 1024` in the request payload to `POST /openai/v1/embeddings`.
2. Upon receiving each vector $v \in \mathbb{R}^d$:
   - If $d > 1024$ (e.g., an upstream fallback ignored the `dimensions` parameter and returned 2048-d), slice the Matryoshka prefix $v_{1:1024}$ and apply L2 normalization: $\hat{v} = \frac{v_{1:1024}}{\|v_{1:1024}\|_2 + \epsilon}$.
   - If $d == 1024$, apply L2 normalization $\hat{v} = \frac{v}{\|v\|_2 + \epsilon}$ to guarantee unit-norm cosine similarity.
   - If $d < 1024$ (e.g., a 384-d or 768-d fallback model), raise `DimensionMismatchError` and abort the batch rather than zero-padding incompatible vector spaces into the 1024-d index.

---

## 3. Storage Topology & Multi-Domain Database Schema

```
data/omnilake/
├── lancedb/                         # LanceDB Disk Storage (Apache Arrow / Lance format)
│   └── knowledge_chunks.lance       # Unified multi-domain vector + BM25 FTS table
├── causal_graphs/                   # Causal Mechanism Graph Storage
│   ├── graph_edges.parquet          # DuckDB-queryable causal triples Parquet file
│   └── kuzu_db/                     # Embedded KuzuDB property graph directory
└── raw_transcripts/                 # Symlink or reference to ./transcripts/*.vtt
```

### 3.1 Unified LanceDB Schema (`knowledge_chunks.lance`)

All domains share one physical Arrow schema (`knowledge_chunks`) so cross-domain queries (e.g., *"Which fermented foods or probiotics improve skin barrier and mitochondrial autophagy?"*) execute in a single vectorized pass, while single-domain queries pre-filter on `domain` in $<1\text{ ms}$.

```python
import pyarrow as pa

KNOWLEDGE_CHUNKS_SCHEMA = pa.schema([
    pa.field("chunk_id", pa.string()),             # Primary key: "{domain}_{source_id}_{tier}_{idx:04d}"
    pa.field("domain", pa.string()),               # "medical" | "seo" | "fermentation"
    pa.field("sub_domain", pa.string()),           # e.g., "mitochondria", "brain_health", "skin_aging", "lacto_recipe"
    pa.field("chunk_tier", pa.string()),           # "distilled_claim" (Track A) | "verbatim_window" (Track B)
    pa.field("source_type", pa.string()),          # "youtube" | "pubmed" | "guideline" | "recipe"
    pa.field("source_id", pa.string()),            # 11-char YouTube video_id or PubMed PMID
    pa.field("channel_or_journal", pa.string()),   # e.g., "Huberman Lab", "Peter Attia MD"
    pa.field("title", pa.string()),                # Video or paper title
    pa.field("timestamp_start", pa.float32()),     # Start time in seconds (e.g., 2535.0)
    pa.field("timestamp_end", pa.float32()),       # End time in seconds (e.g., 2595.0)
    pa.field("citation_url", pa.string()),         # "https://youtu.be/{source_id}?t={int(timestamp_start)}"
    pa.field("context_header", pa.string()),       # "[Domain: medical/mitochondria] [Channel: ...] [Title: ...] [Time: 42m15s]"
    pa.field("clean_text", pa.string()),           # context_header + raw_text (used for Embedding & BM25 FTS)
    pa.field("raw_text", pa.string()),             # Pure chunk body without header (for UI display & citation cards)
    pa.field("vector", pa.list_(pa.float32(), 1024)),  # L2-normalized 1024-d Nemotron 3 Embed 1B vector
    pa.field("entities", pa.list_(pa.string())),   # Canonical pathways/tags (e.g., ["AMPK", "mTOR", "Mitophagy"])
    pa.field("authority_score", pa.float32()),     # 0.0 to 1.0 domain credibility weight (generalized EBM score)
    pa.field("sponsorship_audited", pa.bool_()),   # True if passed Triple-Shield Sponsor Audit
    pa.field("domain_meta", pa.string()),          # JSON object with domain-specific telemetry for DuckDB SQL
    pa.field("file_hash", pa.string()),            # Foreign key to subflow_state.db jobs.file_hash
    pa.field("created_at", pa.float64()),          # Unix epoch timestamp
])
```

### 3.2 `domain_meta` Polymorphic JSON Contract (Queryable via DuckDB)

DuckDB queries `knowledge_chunks.lance` directly via Apache Arrow zero-copy integration and extracts domain-specific attributes using `json_extract_string(domain_meta, '$.key')`:

* **`medical` (`MitoLake`)**:
  ```json
  {
    "evidence_level": "mechanistic_seminar",
    "target_systems": ["mitochondria", "skin", "brain"],
    "interventions": ["Urolithin A", "NMN", "Zone 2 Cardio"],
    "biomarkers": ["NAD+/NADH", "VO2max", "hs-CRP"],
    "section_category": "2. Proposed Methodology & Frameworks"
  }
  ```
* **`seo` (`RankLake`)**:
  ```json
  {
    "platform": "short_form_video",
    "framework_name": "3-2-1 Method",
    "empirical_lift": "2.7x retention",
    "section_category": "3. Key Findings & Empirical Data"
  }
  ```
* **`fermentation` (`ZymoLake`)**:
  ```json
  {
    "starter_culture": "Lactobacillus plantarum",
    "temp_celsius": 22.0,
    "salinity_pct": 2.5,
    "target_ph": 4.2,
    "duration_days": 7
  }
  ```

### 3.3 Causal Graph Schema (`causal_graphs/graph_edges.parquet` & KuzuDB)

Each **Causal Triple** extracted from structured notes or transcripts is stored in `graph_edges.parquet` (for instant DuckDB recursive CTE / join queries) and mirrored into KuzuDB (`causal_graphs/kuzu_db/`):

| Column | Type | Description & Example |
| :--- | :--- | :--- |
| `edge_id` | `STRING` | Deterministic SHA-1 of `(domain, source_id, subject, predicate, object, timestamp_start)` |
| `domain` | `STRING` | `"medical"` \| `"seo"` \| `"fermentation"` |
| `sub_domain` | `STRING` | `"mitochondria"`, `"skin_aging"`, `"lacto_recipe"`, etc. |
| `subject` | `STRING` | Canonical source entity (e.g., `"Urolithin A"`, `"2.5% Salinity"`) |
| `predicate` | `STRING` | `"ACTIVATES"` \| `"INHIBITS"` \| `"UPREGULATES"` \| `"DOWNREGULATES"` \| `"REQUIRES"` \| `"PRODUCES"` \| `"PREVENTS"` |
| `object` | `STRING` | Canonical target entity (e.g., `"Mitophagy"`, `"Skin Collagen Degradation"`) |
| `evidence_text` | `STRING` | Supporting claim sentence |
| `source_id` | `STRING` | `video_id` or `PMID` |
| `timestamp_start` | `FLOAT` | Exact timestamp in seconds |
| `citation_url` | `STRING` | `https://youtu.be/{source_id}?t={int(timestamp_start)}` |
| `authority_score` | `FLOAT` | Inherited credibility weight (`0.0–1.0`) |
| `chunk_id` | `STRING` | Foreign key to `knowledge_chunks.lance` |

### 3.4 SQLite State Queue Extension (`subflow_state.db`)

[`pipeline.py`](file:///Users/jameskim/Projects/Subflow/pipeline.py#L116-L131)'s `jobs` table is backward-compatibly extended via `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` logic:
* `domain TEXT DEFAULT 'medical'`
* `sub_domain TEXT DEFAULT 'general'`
* `channel_name TEXT DEFAULT ''`
* `video_title TEXT DEFAULT ''`
* `index_status TEXT DEFAULT 'PENDING'` (`PENDING` | `INDEXED` | `FAILED`)
* `chunks_indexed INTEGER DEFAULT 0`

---

## 4. Domain Cartridge Registry Specification

Domain Cartridges live in a declarative registry (`domains.py`) so adding a new domain requires zero changes to `collector.py`, `pipeline.py`, `indexer.py`, or `retriever.py`.

### 4.1 Cartridge 1: `medical` (`MitoLake`)
* **Target Sub-Domains**:
  * `anti_aging` (항노화 / Longevity pathways: AMPK, mTOR, Sirtuins, Senolytics)
  * `mitochondria` (미토콘드리아 건강: Mitophagy, OXPHOS, ATP, NAD+, CoQ10, PQQ, Urolithin A, Cardiolipin)
  * `brain_health` (뇌 건강: BDNF, Neurogenesis, Glymphatic clearance, Blood-Brain Barrier, Ketones, EPA/DHA)
  * `antioxidant` (항산화: Nrf2, Glutathione, NAC, GlyNAC, SOD, Catalase, Astaxanthin, Ergothioneine, ROS hormesis)
  * `skin_aging` (피부 노화 방지: Collagen Type I/III, MMP-1 inhibition, ECM, UV photoaging, Autophagy in fibroblasts, Glycation/AGEs, Retinoids, Niacinamide)
* **Authority Scoring Rules**:
  * Peer-reviewed Meta-Analysis / RCT (`source_type="pubmed"`): `0.90 – 0.98`
  * Verified MD/PhD Academic Seminar Channels (`Huberman Lab`, `Peter Attia MD`, `FoundMyFitness`, `Matt Kaeberlein`, `Physionic`, `MedCram`): `0.75 – 0.85`
  * Standard Audited Educational YouTube Transcript: `0.60`

### 4.2 Cartridge 2: `seo` (`RankLake`)
* **Target Sub-Domains**: `short_form_algorithm`, `technical_seo`, `content_funnel`, `hook_engineering`, `conversion_copy`
* **Canonical Entities**: `3-2-1 Method`, `Bullseye Ring 0`, `Outlier Score`, `Retention Curve`, `CTR`, `Search Intent`, `Core Web Vitals`, `E-E-A-T`
* **Authority Scoring Rules**:
  * Empirical Case Study with Verified Analytics (`>50 videos` / A-B cohort metrics): `0.80 – 0.90`
  * General Practitioner Commentary: `0.60`

### 4.3 Cartridge 3: `fermentation` (`ZymoLake`)
* **Target Sub-Domains**: `lacto_fermentation`, `koji_enzymatic`, `vinegar_acetic`, `sourdough_wild_yeast`, `probiotic_beverage`
* **Canonical Entities**: `Lactobacillus plantarum`, `Lactobacillus brevis`, `Leuconostoc mesenteroides`, `Aspergillus oryzae`, `Acetobacter`, `Salinity %`, `pH`, `Anaerobic`, `GABA`, `Organic Acids`, `Postbiotics`
* **Authority Scoring Rules**:
  * Quantitative Protocol (explicit weight %, temperature °C, pH threshold): `0.85 – 0.95`
  * Culinary / Traditional Unmeasured Recipe: `0.60`

---

## 5. 4-Stage Ingestion & Dual-Track Chunking Pipeline

```
[Raw VTT/SRT + Structured Markdown Notes]
                  │
                  ▼
[Stage 1: Triple-Shield Sponsor & Rolling Caption Auditor]
  ├─ Shield 1: SponsorBlock API exact interval excision
  ├─ Shield 2: VTT Rolling Overlap removal (extract_non_overlapping_delta)
  └─ Shield 3: Bilingual SPONSOR_REGEX + 60s-90s smart boundary skip
                  │
        ┌─────────┴─────────┐
        ▼                   ▼
[Stage 2A: Track A]   [Stage 2B: Track B]
  Distilled Claims      Verbatim Semantic Windows
  (from *.md bullets    (Sentence-aligned 150-250 words /
   + Causal Triples)     400-700 tokens, 30-word overlap)
        └─────────┬─────────┘
                  ▼
[Stage 3: Contextual Header + Entity Tagging]
  ├─ Prepend "[Domain: {d}/{sd}] [Channel: {ch}] [Title: {t}] [Time: {MM}m{SS}s]"
  └─ Match canonical bilingual entities & compute authority_score
                  │
                  ▼
[Stage 4: jpro-router Embedding & Hybrid Lakehouse Upsert]
  ├─ POST /openai/v1/embeddings (model="gpt-embedding", dimensions=1024)
  ├─ Upsert to LanceDB (knowledge_chunks.lance) + rebuild BM25 FTS index
  └─ Upsert Causal Triples to causal_graphs/graph_edges.parquet & KuzuDB
```

### 5.1 Stage 1: Triple-Shield Sponsor & Noise Auditor
1. **Shield 1 (Crowd-Sourced Intervals)**: Queries `fetch_sponsorblock(video_id)` for `["sponsor", "intro", "outro", "selfpromo", "interaction"]` and drops any cue whose `start_sec` falls inside a skip interval.
2. **Shield 2 (Rolling Caption Deduplication)**: Applies Subflow's `extract_non_overlapping_delta(prev_text, curr_line)` to strip repeated lines caused by YouTube's 2–3 line scrolling captions, plus strips sound cues (`[Music]`, `[박수]`, `(웃음)`) and conversational fillers (`uh`, `um`, `you know`).
3. **Shield 3 (Bilingual Regex & Context Boundary Slicing)**:
   ```python
   SPONSOR_REGEX = re.compile(
       r"(discount code|promo code|sponsored by|brought to you by|athletic greens|\bag1\b|"
       r"use code|20%\s*off|link in the description|lmnt|helix sleep|insidetracker|eight sleep|"
       r"제휴\s*링크|할인\s*코드|협찬|유료\s*광고\s*포함|고정\s*댓글\s*링크|더보기란\s*링크)",
       re.IGNORECASE,
   )
   ```
   When `SPONSOR_REGEX` matches at cue timestamp $t_0$, the auditor skips cues until $\min(t_0 + 90.0\text{s}, t_{\text{topic\_shift}})$, where $t_{\text{topic\_shift}}$ is the first pause $\Delta t \ge 4.0\text{s}$ after $t_0 + 30.0\text{s}$.

### 5.2 Stage 2A: Track A (`distilled_claim` & Causal Triple Extraction)
When [`pipeline.py`](file:///Users/jameskim/Projects/Subflow/pipeline.py) finishes generating and auditing `structured_notes/{title}_[{video_id}].md`:
1. The parser walks each section (`### 1.` through `### 5.`) and groups each top-level bullet (`- **Claim Title**: ... [MM:SS]`) together with its indented sub-bullets (`  - **Mechanism / Protocol**: ... [MM:SS]`).
2. It extracts the earliest cited `[MM:SS]` as `timestamp_start` and the latest cited `[MM:SS]` (or `timestamp_start + 60.0`) as `timestamp_end`.
3. Each self-contained bullet group becomes a `chunk_tier="distilled_claim"` **Atomic Chunk** with a `+0.10` authority bonus (capped at `0.95`) because it passed Subflow's 2-Stage Deterministic + LLM Judge Quality Gate.
4. **Causal Triple Extraction**: Using deterministic pattern matching (`A activates/inhibits/upregulates/reduces B`) plus an optional structured extraction pass on Section 2 & Section 3 bullets, each causal mechanism is emitted as a `Causal Triple` `(subject, predicate, object, timestamp_start, chunk_id)`.

### 5.3 Stage 2B: Track B (`verbatim_window` Sentence-Aligned Chunker)
Directly from the Stage 1 cleaned cues:
1. Cues are accumulated along sentence boundaries (`.`, `?`, `!`, `다.`) up to `target_words = 180` (~450–650 tokens depending on script/language) with `overlap_words = 35` (~50 tokens).
2. `timestamp_start` is the exact `start` of the first cue in the window; `timestamp_end` is the `end` of the last cue in the window.
3. Enriched with the Contextual Retrieval header:
   ```text
   [Domain: medical/mitochondria] [Channel: Huberman Lab] [Title: How to Enhance Mitochondria] [Time: 42m15s-43m28s] {chunk_body}
   ```

---

## 6. Millisecond Hybrid Inference Pipeline (`Embed -> Hybrid RRF + Graph -> Rerank -> Lightning`)

When a user or downstream agent queries OmniLake (in Korean or English), `retriever.py` executes a 4-step pipeline designed for **sub-second retrieval and millisecond reranking**:

```
Query (KR/EN)
  │
  ├──► 1. Embed Query via jpro-router (POST /openai/v1/embeddings, model="gpt-embedding", dimensions=1024)
  │
  ├──► 2. Hybrid Candidate Retrieval (Top-40 Candidates, <15ms local disk I/O)
  │      ├─ (a) LanceDB Pre-filtered Dense Search:
  │      │      WHERE domain IN (...) AND authority_score >= min_score AND sponsorship_audited = true
  │      ├─ (b) LanceDB Native BM25 Full-Text Search (exact biochemical/recipe terms: NAD+, Urolithin A, pH 4.2)
  │      ├─ (c) Reciprocal Rank Fusion (RRF, k=60) combining Dense + BM25 + chunk_tier boost (distilled_claim 1.15x)
  │      └─ (d) DuckDB / KuzuDB 1-Hop Causal Graph Expansion on matched entities
  │
  ├──► 3. Cross-Encoder Reranking via jpro-router (POST /openai/v1/rerank, model="gpt-rerank")
  │      └─ Re-scores Top-25 candidates against the query → selects Top-K (default K=6) high-precision chunks
  │
  └──► 4. Lightning Synthesis via jpro-router (POST /openai/v1/chat/completions, model="gpt-6-luna")
         └─ Synthesizes answer strictly from Top-K chunks + Causal Triples and attaches Evidence Cards:
            • [Source Title | Channel] (MM:SS) → https://youtu.be/{video_id}?t={start_sec}
            • Authority Score & Causal Pathway Tag ([AMPK → Mitophagy])
```

### 6.1 Reciprocal Rank Fusion (RRF) & Authority Weighting Formula
For candidate chunk $c$ with dense rank $r_{\text{dense}}(c)$, BM25 rank $r_{\text{bm25}}(c)$, authority score $\alpha(c) \in [0, 1]$, and tier multiplier $\tau(c)$ (`1.15` for `distilled_claim`, `1.00` for `verbatim_window`):

$$\text{Score}_{\text{hybrid}}(c) = \tau(c) \cdot \left(1 + 0.2 \cdot \alpha(c)\right) \cdot \left( \frac{1}{60 + r_{\text{dense}}(c)} + \frac{1}{60 + r_{\text{bm25}}(c)} \right)$$

After Stage 3 Cross-Encoder Reranking (`gpt-rerank` relevance score $s_{\text{rerank}}(c)$):

$$\text{Score}_{\text{final}}(c) = 0.75 \cdot s_{\text{rerank}}(c) + 0.25 \cdot \alpha(c)$$

---

## 7. File & Module Architecture Specification

To keep the codebase modular, testable, and aligned with Subflow's single-responsibility philosophy, the implementation is structured into 4 clean modules:

| File | Responsibility |
| :--- | :--- |
| `domains.py` | Declarative **Domain Cartridge** registry (`medical`/`MitoLake`, `seo`/`RankLake`, `fermentation`/`ZymoLake`), bilingual `SPONSOR_REGEX`, canonical entity dictionaries, authority scoring rules, and domain-aware system prompts. |
| `indexer.py` | **OmniLake Indexer**: Triple-Shield VTT parser (`parse_and_clean_segments`), Track B `chunk_youtube_segments`, Track A `parse_structured_markdown_chunks` + causal triple extractor, `jpro-router` `/openai/v1/embeddings` client with 1024-d MRL guard, LanceDB (`knowledge_chunks.lance`) + BM25 FTS + DuckDB/Parquet (`graph_edges.parquet`) + KuzuDB upsert engine. |
| `retriever.py` | **OmniLake Retriever & Lightning Engine**: `jpro-router` `/connect` auto-discovery, Hybrid Dense + BM25 + DuckDB prefilter search, 1-hop Causal Triple lookup, `/openai/v1/rerank` (`gpt-rerank`) precision reranker, and `/openai/v1/chat/completions` (`gpt-6-luna`) synthesis with timestamp Evidence Cards. |
| `collector.py` & `pipeline.py` | Extended with `--domain` (`medical`, `seo`, `fermentation`), `--sub-domain`, `--channel`, and `--index` flags so `python collector.py <URL> --domain medical --sub-domain mitochondria --run-pipeline --index` executes end-to-end from collection to LanceDB indexing in one command. |

---

## 8. Verification & Acceptance Criteria

1. **Zero Remote Leak Check**: `indexer.py` and `retriever.py` resolve all endpoints via `http://127.0.0.1:4001/connect` and use `gpt-embedding`, `gpt-rerank`, `gpt-6-luna`, and `gpt-6.1-sol`.
2. **Sponsor Contamination Test**: Feeding a transcript segment containing `"brought to you by AG1, use code HUBERMAN for 20% off"` must result in 0 chunks containing `"AG1"` or `"20% off"`, and all persisted rows have `sponsorship_audited = True`.
3. **1024-Dim Vector Invariant Test**: Every row inserted into `knowledge_chunks.lance` has `len(row["vector"]) == 1024` and L2 norm $\approx 1.0 \pm 10^{-4}$.
4. **Multi-Domain Isolation & Cross-Domain Query Test**:
   - Ingesting a `medical` transcript and an `seo` transcript into `knowledge_chunks.lance` allows `domain="medical"` pre-filtered retrieval to return 100% medical chunks.
   - Cross-domain query (`domain=None`) retrieves across `medical` and `fermentation` seamlessly.
5. **End-to-End Inference Latency & Citation Grounding**:
   - Querying `retriever.py` executes Embed (`gpt-embedding`) $\rightarrow$ LanceDB Hybrid Search + Causal Graph $\rightarrow$ Rerank (`gpt-rerank`) $\rightarrow$ Lightning (`gpt-6-luna`) and returns structured Markdown with valid `https://youtu.be/{video_id}?t={sec}` timestamp Evidence Cards.

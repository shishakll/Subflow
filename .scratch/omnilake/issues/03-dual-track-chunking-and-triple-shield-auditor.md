# 03: Lock Dual-Track Chunking and Triple-Shield Sponsor Defense

Type: grilling
Status: resolved
Blocked by: 02

## Question

How should Subflow combine its existing SponsorBlock + VTT rolling-caption deduplication (`clean_transcript`) and structured Markdown reports (`structured_notes/*.md`) with the new semantic window chunker and sponsor regex auditor?

## Answer

1. **Triple-Shield Sponsor & Noise Auditor**:
   - Shield 1: Crowd-sourced SponsorBlock intervals (`fetch_sponsorblock`).
   - Shield 2: VTT rolling-caption suffix-prefix deduplication (`extract_non_overlapping_delta`).
   - Shield 3: Bilingual `SPONSOR_REGEX` with smart context-boundary slicing (60–90s window).
2. **Dual-Track Indexing**:
   - **Track B (`verbatim_window`)**: Sentence-aligned 400–700 token windows (35-word overlap) with Contextual Retrieval headers (`[Domain] [Channel] [Title] [Time]`).
   - **Track A (`distilled_claim` + `Causal Triple`)**: High-density hierarchical bullets and causal relationships parsed from `structured_notes/*.md` with `[MM:SS]` grounding.
   - Support both `--fast-only` (Track B) and `--deep` (Track A + Track B + Causal Graph) ingestion modes.

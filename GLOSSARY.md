# Subflow MitoLake

Subflow MitoLake transforms long-form multimedia medical seminars and transcripts (anti-aging, mitochondrial health, brain health, antioxidant defense, skin aging prevention) into noise-free, timestamp-grounded atomic medical knowledge units and causal mechanisms for precision retrieval and reasoning.

## Language

### Knowledge Architecture

**MitoLake**:
The dedicated medical longevity knowledge lakehouse that unifies distilled medical claims, verbatim transcript windows, and causal mechanisms under a single queryable domain model.
_Avoid_: OmniLake, RankLake, ZymoLake, Vector DB, RAG Store

**Atomic Chunk**:
A self-contained, sponsor-free unit of medical knowledge bound to an exact source identifier and start-to-end timestamp interval.
_Avoid_: Segment, Text Block, Snippet

**Distilled Claim**:
A high-density Atomic Chunk synthesized from structured analytical notes that states a single medical thesis, molecular mechanism, empirical clinical metric, or actionable protocol step with timestamp grounding.
_Avoid_: Summary Bullet, Note Fragment

**Verbatim Window**:
A context-enriched Atomic Chunk formed by merging consecutive, de-duplicated spoken transcript sentences over a bounded time window.
_Avoid_: Raw Subtitle, Caption Slice

**Authority Score**:
A normalized 0.0 to 1.0 credibility weight assigned to a source or Atomic Chunk based on medical evidence standards (e.g., meta-analyses, clinical trials, or seminars by verified MD/PhD researchers).
_Avoid_: Trust Rating, Confidence Score

**Causal Triple**:
A directed relationship `(Subject)-[Predicate]->(Object)` capturing how a molecule, lifestyle intervention, or condition activates, inhibits, requires, or produces a downstream biological outcome (e.g., `Urolithin A` -> `ACTIVATES` -> `Mitophagy`).
_Avoid_: Knowledge Graph Edge, Relation Tuple

**Sponsor Contamination**:
Commercial promotions, supplement discount pitches (e.g., AG1, promo codes), or affiliate calls-to-action embedded inside educational content that could distort factual retrieval or lead to commercial hallucination.
_Avoid_: Ad Noise, Spam Text

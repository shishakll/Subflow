# Subflow OmniLake

Subflow OmniLake transforms long-form multimedia transcripts and primary literature across multiple knowledge domains (medical longevity, SEO, fermentation science) into noise-free, timestamp-grounded atomic knowledge units and causal mechanisms for precision retrieval and reasoning.

## Language

### Knowledge Architecture

**OmniLake**:
The global multi-domain knowledge lakehouse that unifies distilled claims, verbatim transcript windows, and causal mechanisms under a single queryable domain model.
_Avoid_: MitoLake (when referring to the global engine across all domains), Vector DB, RAG Store

**Domain Cartridge**:
A pluggable domain specification that defines the ontology, high-trust sources, authority scoring rules, and extraction prompts for a single vertical (such as `MitoLake` for medical longevity, `RankLake` for SEO, or `ZymoLake` for fermentation recipes).
_Avoid_: Domain Config, Category Profile, Topic Filter

**MitoLake**:
The medical longevity Domain Cartridge covering anti-aging, mitochondrial health, brain health, antioxidant defense, and skin aging prevention.
_Avoid_: Medical DB, YouthCore DB

**RankLake**:
The search and content growth Domain Cartridge covering algorithm dynamics, audience conversion funnels, and hook engineering.
_Avoid_: SEO DB, Marketing Store

**ZymoLake**:
The fermentation science Domain Cartridge covering microbial starters, enzymatic transformations, and quantitative fermentation protocols.
_Avoid_: Recipe DB, Cooking Store

### Knowledge Units & Quality

**Atomic Chunk**:
A self-contained, sponsor-free unit of knowledge bound to an exact source identifier and start-to-end timestamp interval.
_Avoid_: Segment, Text Block, Snippet

**Distilled Claim**:
A high-density Atomic Chunk synthesized from structured analytical notes that states a single thesis, mechanism, empirical metric, or actionable protocol step with timestamp grounding.
_Avoid_: Summary Bullet, Note Fragment

**Verbatim Window**:
A context-enriched Atomic Chunk formed by merging consecutive, de-duplicated spoken transcript sentences over a bounded time window.
_Avoid_: Raw Subtitle, Caption Slice

**Authority Score**:
A normalized 0.0 to 1.0 credibility weight assigned to a source or Atomic Chunk based on domain-specific evidence standards (such as clinical evidence hierarchy in medicine, empirical case-study metrics in SEO, or measured pH/salinity reproducibility in fermentation).
_Avoid_: EBM Score (outside the medical domain), Trust Rating, Confidence Score

**Causal Triple**:
A directed relationship `(Subject)-[Predicate]->(Object)` capturing how an intervention, condition, or entity activates, inhibits, requires, or produces a downstream outcome.
_Avoid_: Knowledge Graph Edge, Relation Tuple

**Sponsor Contamination**:
Commercial promotions, discount pitches, affiliate calls-to-action, or self-promotional segments embedded inside educational content that could distort factual retrieval or lead to commercial hallucination.
_Avoid_: Ad Noise, Spam Text

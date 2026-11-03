# Dedicated MitoLake Medical Lakehouse and Local Proxy Socket Routing

Status: accepted

Subflow MitoLake stores medical longevity knowledge (anti-aging, mitochondrial health, brain health, antioxidant defense, skin aging prevention) in a unified LanceDB table (`knowledge_chunks.lance`) paired with DuckDB out-of-core SQL filtering (`domain="medical"`, `sub_domain`, `authority_score`, and JSON `domain_meta`) and KuzuDB/Parquet causal triples. All other domains (such as SEO and fermentation) are excluded to ensure zero domain contamination and zero model confusion. All model inference routes exclusively through `jpro-router` (`http://127.0.0.1:4001/openai/v1`) using dedicated virtual sockets: `gpt-embedding` (`/openai/v1/embeddings` with `dimensions=1024` Matryoshka projection), `gpt-rerank` (`/openai/v1/rerank`), `gpt-6.1-sol` (`/openai/v1/chat/completions` for 1M-context structuring), and `gpt-6-luna` (`/openai/v1/chat/completions` for millisecond Lightning synthesis).

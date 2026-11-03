# 01: Verify and wire jpro-router OpenAI Embedding and Rerank sockets

Type: task
Status: resolved
Blocked by: None

## Question

Which `jpro-router` (`/Users/jameskim/Projects/jpro-router`) OpenAI endpoints and virtual sockets should Subflow connect to for Embedding, Reranking, Structuring, and Lightning Inference, and how do we ensure 1024-dimension vector consistency across the fallback chain?

## Answer

1. **Endpoints & Sockets Wired and Verified Live on `http://127.0.0.1:4001`**:
   - **Discovery**: `GET /connect` exposes `embeddings_url` (`/openai/v1/embeddings`), `rerank_url` (`/openai/v1/rerank`), and `chat_completions_url` (`/openai/v1/chat/completions`).
   - **Embedding**: `POST /openai/v1/embeddings` with `model="gpt-embedding"` and `"dimensions": 1024` routes to `nvidia/nemotron-3-embed-1b:free` (with key rotation and fallback chain) and returns native 1024-d Matryoshka vectors.
   - **Rerank**: `POST /openai/v1/rerank` with `model="gpt-rerank"` routes to `nvidia/llama-nemotron-rerank-vl-1b-v2:free` and returns `results[{index, relevance_score, document}]`.
   - **Structuring & Lightning**: `POST /openai/v1/chat/completions` with `model="gpt-6.1-sol"` (1M context structuring) and `model="gpt-6-luna"` (`nvidia/nemotron-3.5-lightning:free` fast inference).
2. **Dimension Guard**: Pass `"dimensions": 1024` on request, slice prefix `[:1024]` + L2-normalize if an upstream fallback returns `>1024` dims, and reject `<1024` dims.

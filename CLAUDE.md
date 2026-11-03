# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**Subflow** is a high-performance local batch pipeline that structures YouTube long-form transcripts (1-3+ hours of academic seminars, tech podcasts, deep interviews) into precision Markdown research reports at **$0.00 cost** via a local `jpro-router` proxy (`http://127.0.0.1:4001`).

### Core Architecture
- **Single-pass pipeline**: Local deterministic preprocessing → One LLM call → Markdown output
- **jpro-router black-box**: 10+ OpenRouter account rotation, Least-In-Flight scheduling, 429 handling, fallback chain — all handled by the proxy. Subflow only uses standard OpenAI-compatible client.
- **Deterministic grounding**: 60-second interval `[MM:SS]` timestamps injected into transcript; LLM forced to cite timestamps for every claim.
- **Resumable/idempotent**: SQLite state machine survives crashes, network failures, SIGKILL.

## Commands

```bash
# Install dependencies
pip install -r requirements.txt

# Place .vtt/.srt files in transcripts/ (recommended naming: Title_[11charID].vtt)
# Run pipeline
python pipeline.py
```

Output: Structured Markdown notes in `structured_notes/`. Processing history in `subflow_state.db`.

## Key Files

| File | Purpose |
|------|---------|
| `pipeline.py` | Core pipeline: discovery, VTT cleaning, SponsorBlock, 2-thread batch worker |
| `requirements.txt` | `openai>=1.0.0`, `requests>=2.28.0` |
| `TECH_SPEC.md` | Full technical specification (architecture, algorithms, failure modes) |
| `README.md` | Quick start guide |
| `subflow_state.db` | SQLite job queue (auto-created) |
| `transcripts/` | Input directory for subtitle files |
| `structured_notes/` | Output directory for generated reports |

## Critical Invariants (Do Not Break)

1. **Never modify jpro-router code** — it's external infrastructure. Subflow only calls `/connect` and `/openai/v1/chat/completions`.
2. **Model must be `gpt-6.1-sol`** — this virtual socket maps to the 1M context free model pool inside jpro-router. Never pass upstream model names (e.g., `nvidia/nemotron-3-ultra...`).
3. **SAFE_CHAR_LIMIT = 400,000** — hard cap on cleaned transcript length to prevent context overflow on fallback models (128K context).
4. **5 required sections + timestamp citations** — validated by `validate_markdown_output()`; failures retry (max 5) then mark `FAILED`.
5. **Concurrency = 2 workers** — with 1.0s inter-task jitter to let jpro-router's Least-In-Flight rebalance.

## Pipeline Flow

```
transcripts/*.vtt → SHA-256 hash → SQLite queue (PENDING)
    → clean_transcript(): SponsorBlock skip + Rolling caption dedup + 60s [MM:SS] anchors
    → Zero-config: GET /connect → OpenAI client bound to local proxy
    → Single chat.completions.create(model="gpt-6.1-sol", temperature=0.15)
    → validate_markdown_output(): length≥150, 5 headers, ≥1 [MM:SS] citation
    → Write structured_notes/*.md + SQLite status=DONE
```

## Development Notes

- **No lint/typecheck/test commands exist** — this is a focused single-script pipeline.
- To test changes: drop a `.vtt` in `transcripts/`, run `python pipeline.py`, inspect `structured_notes/*.md`.
- The `clean_transcript()` function is the most complex piece — it handles VTT parsing, SponsorBlock intervals, suffix-prefix overlap removal for rolling captions, and timestamp anchor injection.
- `resolve_output_path()` prevents filename collisions by appending hash suffix when same filename has different content.
- Worker sleep in `finally` block (1.0s) is intentional for jpro-router scheduling — don't remove.

## Environment Variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `JPRO_ROUTER_HOST` | `http://127.0.0.1:4001` | Proxy base URL |
| `LOCAL_PROXY_AUTH_TOKEN` | `local-token` | Fallback auth if `/connect` fails |

## Agent skills

### Issue tracker

Issues live as markdown files in `.scratch/`. See `docs/agents/issue-tracker.md`.

### Triage labels

Default five canonical labels (`needs-triage`, `needs-info`, `ready-for-agent`, `ready-for-human`, `wontfix`). See `docs/agents/triage-labels.md`.

### Domain docs

Single-context layout (`GLOSSARY.md` + `docs/adr/` at repo root). See `docs/agents/domain.md`.
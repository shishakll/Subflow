import os
import re
import json
import time
import sqlite3
import hashlib
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from openai import OpenAI

# ==============================================================================
# Subflow Engine (jpro-router 전용 고속 자막 구조화 파이프라인)
#
# 원칙:
# 1. jpro-router(http://127.0.0.1:4001) 코드는 일체 수정하지 않고 엔드포인트로만 사용합니다.
# 2. 10개 이상의 계정 로테이션, 429 처리, Fallback 모델 전환은 jpro-router가 전담합니다.
# 3. Subflow는 [SponsorBlock/VTT 정제 -> gpt-6.1-sol 단일 호출 -> 마크다운 저장]만 수행합니다.
# ==============================================================================

ROUTER_HOST = os.environ.get("JPRO_ROUTER_HOST", "http://127.0.0.1:4001")
DB_PATH = Path("subflow_state.db")
INPUT_DIR = Path("./transcripts")
OUTPUT_DIR = Path("./structured_notes")

INPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ── 1. jpro-router Zero-Config 디스커버리 ────────────────────────────────────
def resolve_router_client() -> tuple[OpenAI, str]:
    """
    jpro-router의 무인증 /connect 엔드포인트를 호출하여
    토큰과 엔드포인트 URL, 기본 가상 슬롯(gpt-6.1-sol)을 자동 취득합니다.
    """
    connect_url = f"{ROUTER_HOST}/connect"
    try:
        res = requests.get(connect_url, timeout=3)
        if res.status_code == 200:
            data = res.json()
            base_url = data["endpoints"]["openai"]["base_url"]
            auth_token = data.get("auth_token", "")
            slot_model = data["endpoints"]["openai"]["default_model"]
            print(f"[jpro-router] 로컬 프록시 연결 감지 성공: {base_url} (슬롯: {slot_model})")
            client = OpenAI(base_url=base_url, api_key=auth_token or "local-token")
            return client, slot_model
    except Exception as e:
        print(f"[jpro-router] /connect 자동 감지 실패 ({e}) -> 기본값 폴백 적용")

    fallback_token = os.environ.get("LOCAL_PROXY_AUTH_TOKEN", "local-token")
    client = OpenAI(base_url=f"{ROUTER_HOST}/openai/v1", api_key=fallback_token)
    return client, "gpt-6.1-sol"

# ── 2. English Prompt & Hybrid Quality Gate Specifications ────────────────────
SYSTEM_PROMPT = """You are a Principal Research Analyst and Technical Writer specializing in dissecting long-form academic seminars, technical podcasts, and deep-dive business/engineering interviews into exhaustive, zero-loss structured Markdown research reports.

Ignore casual greetings, small talk, filler words, and sponsor reads. Capture **every core framework, multi-step methodology, tactical workflow, quantitative metric, case study, and formula/prompt** from the transcript and write an exhaustive, high-density report **strictly in English**.

[MANDATORY RULES - VIOLATION CAUSES IMMEDIATE REJECTION]
1. **100% English Output**: Write the entire report strictly in fluent, high-precision analytical English. Do NOT translate into Korean or any other language.
2. **2-Level Hierarchical Depth (No Flat Summaries)**: Single-line shorthand bullets are prohibited. Every major point in EVERY section (`### 1.` through `### 5.`) MUST use top-level bullets (`- `) with 2-space indented sub-bullets (`  - `):
   - `- **Main Thesis / Framework Name**: High-level definition and core objective [MM:SS].`
     - `  - **Mechanism / Protocol**: Step-by-step operational logic, exact sub-components (e.g., all rings, all buckets, all steps), and why it works [MM:SS].`
     - `  - **Concrete Example / Formula / Prompt**: Exact real-world examples, numbers, bio formulas, or prompts demonstrated in the transcript [MM:SS].`
3. **Exhaustive Methodology Coverage**: Include the origin/meaning of framework names (e.g., what each number in a named method represents), auxiliary tactical formulas (e.g., profile bio 3-line structure, tool settings, filtering thresholds), and all sub-steps without omission.
4. **Strict Timestamp Grounding Across All Segments**: Append exact `[MM:SS]` timestamps from the source transcript to the end of every claim, metric, and step. You MUST include **at least 30+ citations** distributed evenly across the **Early (first third), Middle (second third), and Late (final third)** portions of the transcript. Never fabricate timestamps.
5. **Required 5-Section Structure & High Density**: Include all 5 exact section headers below (`### 1.` through `### 5.`), producing an exhaustive report of **8,000 to 15,000 characters (~1,400 to 2,500 words)**.

=== REQUIRED OUTPUT FORMAT (USE THESE EXACT HEADERS) ===

# [Title: Core Topic & Target Framework / Methodology Name]

### 1. Core Problem & Thesis
- Dissect the fundamental problem being solved, structural flaws in conventional wisdom, the speaker's background/credibility, and the overarching thesis in 4–6 major bullets (each with 2–3 indented sub-bullets and `[MM:SS]` citations).

### 2. Proposed Methodology & Frameworks
- Break down **every major framework, system, and tactical formula** in the transcript as its own dedicated top-level bullet. Under each framework, use indented sub-bullets to detail: (a) Definition & structural components, (b) Step-by-step execution mechanism, and (c) Concrete examples, prompts, or thresholds (`[MM:SS]` citations required).

### 3. Key Findings & Empirical Data
- Document all quantitative results, conversion ratios, A/B comparisons, old-vs-new account benchmarks, filtering thresholds, and complete lists of data-mined patterns (e.g., all 7 hidden patterns with their exact metrics) across 6–10 major bullets with indented sub-bullets (`[MM:SS]` citations required).

### 4. Limitations, Risks & Edge Cases
- Detail 5–7 major operational risks, failure modes (e.g., algorithmic contamination, derivative content traps, automation myths), boundary conditions, and how to mitigate them (`[MM:SS]` citations required).

### 5. Actionable Playbook & Implementation Guide
- Provide a complete, step-by-step replication protocol (`Step 0` through `Step 7`) that allows a practitioner to execute the entire system from scratch without watching the video, including setup, weekly sprint cadence, tracking metrics, and scaling roadmap (`[MM:SS]` citations required).

=== CRITICAL REMINDER ===
Output all 5 section headers (`### 1.` through `### 5.`) verbatim.
Never stop summarizing early; cover the entire transcript from `[00:00]` through the final timestamp.
Finish Section 5 with a complete, well-formed concluding sentence."""

JUDGE_SYSTEM_PROMPT = """You are a strict Quality Assurance Auditor evaluating a structured Markdown research report generated from a long-form video transcript.
Evaluate the report against the provided ground-truth transcript excerpts and return ONLY a single valid JSON object (no markdown fences, no commentary) with the exact keys:
- "pass": boolean (true if score >= 80 and no critical hallucinations/truncations, else false)
- "score": integer (0 to 100, sum of the 4 rubric scores below)
- "scores": object with integer keys "factual_grounding" (0-25), "framework_depth" (0-25), "section_alignment" (0-25), "completeness_and_clarity" (0-25)
- "defects": array of strings (empty array [] if pass is true, otherwise specific actionable fixes)

Scoring Rubric (0-25 points each, total 100):
1. factual_grounding (0-25): Claims, numbers, and mechanisms cited at [MM:SS] accurately reflect the source transcript without fabrication.
2. framework_depth (0-25): Methodologies in Section 2 and Playbook steps in Section 5 explain concrete mechanisms, sub-components, formulas, and examples in 2-level hierarchical depth.
3. section_alignment (0-25): All 5 sections (1: Problem/Thesis, 2: Methodology, 3: Empirical Data, 4: Limitations/Risks, 5: Actionable Playbook) strictly fulfill their distinct analytical goals.
4. completeness_and_clarity (0-25): Written 100% in fluent English, covers early/middle/late parts of the transcript, and ends with a complete sentence without truncation."""

MAX_RETRIES = 5
MAX_OUTPUT_TOKENS = 16384
SAFE_CHAR_LIMIT = 400_000  # ~100K-120K token safety cap for fallback models
REQUIRED_HEADERS = ("### 1.", "### 2.", "### 3.", "### 4.", "### 5.")
CITATION_PATTERN = re.compile(r"\[(\d{1,3}:\d{2}(?::\d{2})?)\]")
TRUNCATED_END_CHARS = ("·", ",", ":", ";", "-", "*", "(", "[", "/")
QUANTITATIVE_PATTERN = re.compile(r"(?:\$\d+|\b\d+(?:[.,]\d+)?(?:\s*%|\s*x|\b))", re.IGNORECASE)
HANGUL_CJK_PATTERN = re.compile(r"[\uac00-\ud7a3\u4e00-\u9fff]")
LATIN_ALPHA_PATTERN = re.compile(r"[a-zA-Z]")

# ── 3. SQLite 상태 큐 ─────────────────────────────────────────────────────────
def init_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
            file_hash TEXT PRIMARY KEY,
            file_name TEXT NOT NULL,
            video_id TEXT,
            cleaned_text TEXT,
            status TEXT DEFAULT 'PENDING',
            retries INTEGER DEFAULT 0,
            updated_at REAL
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);")

    # OmniLake 멀티 도메인 및 인덱싱 상태 컬럼 확장 (하위 호환성 보장)
    for col, col_type in (
        ("domain", "TEXT DEFAULT 'medical'"),
        ("sub_domain", "TEXT DEFAULT 'general'"),
        ("channel_name", "TEXT DEFAULT ''"),
        ("video_title", "TEXT DEFAULT ''"),
        ("index_status", "TEXT DEFAULT 'PENDING'"),
        ("chunks_indexed", "INTEGER DEFAULT 0"),
    ):
        try:
            conn.execute(f"ALTER TABLE jobs ADD COLUMN {col} {col_type}")
        except sqlite3.OperationalError:
            pass  # 이미 컬럼이 존재하는 경우 무시

    conn.commit()
    return conn

# ── 4. 로컬 전처리 (SponsorBlock + VTT 롤링 중복 제거) ────────────────────────
def fetch_sponsorblock(video_id: str) -> list[tuple[float, float]]:
    if not video_id or len(video_id) != 11:
        return []
    url = (
        f"https://sponsor.ajay.app/api/skipSegments?videoID={video_id}"
        f'&categories=["sponsor","intro","outro","selfpromo","interaction"]'
    )
    try:
        res = requests.get(url, timeout=3)
        if res.status_code == 200:
            return [(item["segment"][0], item["segment"][1]) for item in res.json()]
    except Exception:
        pass
    return []

def parse_time(ts: str) -> float:
    parts = ts.replace(",", ".").split(":")
    if len(parts) == 3:
        return int(parts[0]) * 3600 + int(parts[1]) * 60 + float(parts[2])
    elif len(parts) == 2:
        return int(parts[0]) * 60 + float(parts[1])
    return 0.0

def extract_non_overlapping_delta(prev_text: str, curr_text: str) -> str:
    """
    인접한 두 자막 블록(prev_text, curr_text) 간의 Prefix 및 Suffix-Prefix 최장 공통 구간을
    탐지하여 순수 신규 발화(Delta)만 추출합니다. (2줄 이상 롤링 캡션 완벽 대응)
    """
    if not prev_text:
        return curr_text
    if curr_text == prev_text or curr_text in prev_text:
        return ""
    if curr_text.startswith(prev_text):
        return curr_text[len(prev_text):].strip()

    # 단어 단위로 prev_text의 접미사(Suffix)와 curr_text의 접두사(Prefix) 간 최대 겹침 계산
    prev_words = prev_text.split()
    curr_words = curr_text.split()
    max_overlap = min(len(prev_words), len(curr_words))

    for k in range(max_overlap, 0, -1):
        if prev_words[-k:] == curr_words[:k]:
            return " ".join(curr_words[k:]).strip()

    return curr_text

def format_timestamp(sec: float) -> str:
    mm, ss = divmod(int(sec), 60)
    return f"[{mm:02d}:{ss:02d}]"

def clean_transcript(file_path: Path, video_id: str) -> str:
    raw = file_path.read_text(encoding="utf-8", errors="ignore")
    skip_intervals = fetch_sponsorblock(video_id)

    pattern = re.compile(
        r"(\d{1,2}:\d{2}(?::\d{2})?[.,]\d{1,3})\s*-->\s*(\d{1,2}:\d{2}(?::\d{2})?[.,]\d{1,3}).*?\n(.*?)(?=\n\n|\n\d+\n|\Z)",
        re.DOTALL
    )
    matches = pattern.findall(raw)

    if not matches:
        text = re.sub(r"\b(um|uh|you know|어|음|그니까)\b", "", raw, flags=re.IGNORECASE)
        normalized = re.sub(r"\s+", " ", text).strip()
        return normalized[:SAFE_CHAR_LIMIT]

    cleaned_chunks = []
    prev_text = ""
    last_marker = -60.0

    for start_str, _end_str, content in matches:
        start_sec = parse_time(start_str)
        if any(s <= start_sec <= e for s, e in skip_intervals):
            continue

        line = re.sub(r"<[^>]+>", "", content).replace("\n", " ").strip()
        line = re.sub(r"\s+", " ", line)
        if not line or line == prev_text:
            continue

        # 롤링 캡션 중복 제거 (Prefix 및 Suffix-Prefix 슬라이딩 윈도우 병합)
        delta = extract_non_overlapping_delta(prev_text, line)
        prev_text = line

        if not delta:
            continue

        if start_sec - last_marker >= 60.0:
            cleaned_chunks.append(f"\n{format_timestamp(start_sec)} ")
            last_marker = start_sec

        cleaned_chunks.append(delta)

    full_cleaned = " ".join(cleaned_chunks).strip()
    if len(full_cleaned) > SAFE_CHAR_LIMIT:
        full_cleaned = full_cleaned[:SAFE_CHAR_LIMIT]
    return full_cleaned

def parse_anchor_seconds(ts_str: str) -> float:
    """Converts MM:SS or HH:MM:SS string inside brackets to total seconds."""
    parts = [int(p) for p in ts_str.strip().split(":")]
    if len(parts) == 2:
        return float(parts[0] * 60 + parts[1])
    if len(parts) == 3:
        return float(parts[0] * 3600 + parts[1] * 60 + parts[2])
    return 0.0


def validate_markdown_output(
    content: str,
    cleaned_text: str = "",
    finish_reason: str | None = None,
    min_length: int = 150,
) -> dict:
    """
    Stage 1 Deterministic Quality Gate:
    Audits truncation, English language compliance, 5-section order & per-section density,
    2-level hierarchical bullet structure, quantitative metrics in Section 3,
    exact timestamp grounding against cleaned_text anchors, and 3-tercile temporal coverage.
    Returns a dictionary of audit telemetry metrics if all checks pass, or raises ValueError.
    """
    if finish_reason == "length":
        raise ValueError("Output truncated due to max_tokens limit (finish_reason=length).")

    text = (content or "").strip()
    if len(text) < min_length:
        raise ValueError(f"Report length ({len(text)} chars) is below target density ({min_length} chars).")

    if text.endswith(TRUNCATED_END_CHARS):
        raise ValueError(f"Report ends with an incomplete mid-sentence character ('{text[-1]}').")

    # 1. English Language Compliance Gate
    latin_count = len(LATIN_ALPHA_PATTERN.findall(text))
    cjk_count = len(HANGUL_CJK_PATTERN.findall(text))
    if latin_count == 0 or (cjk_count / max(1, latin_count + cjk_count)) > 0.01:
        raise ValueError(
            f"Language compliance failed: output contains non-English CJK/Hangul characters ({cjk_count} chars)."
        )

    # 2. Required 5 Headers & Sequential Order Gate
    header_positions = []
    for h in REQUIRED_HEADERS:
        idx = text.find(h)
        if idx == -1:
            raise ValueError(f"Missing required section header: {h}")
        header_positions.append(idx)

    if header_positions != sorted(header_positions):
        raise ValueError("Section headers (### 1. through ### 5.) are out of sequential order.")

    # 3. Per-Section Density, Hierarchy & Citation Gate
    is_longform = len(cleaned_text) >= 50_000
    min_sec_chars = 600 if is_longform else 80
    min_sec_lines = 4 if is_longform else 2
    nested_bullet_re = re.compile(r"^\s{2,}(?:[-*]|\d+\.)\s+", re.MULTILINE)

    section_Lengths = {}
    for i, start_idx in enumerate(header_positions):
        end_idx = header_positions[i + 1] if i + 1 < len(header_positions) else len(text)
        sec_text = text[start_idx:end_idx].strip()
        sec_lines = [line.strip() for line in sec_text.splitlines()[1:] if line.strip()]
        sec_cites = CITATION_PATTERN.findall(sec_text)
        section_Lengths[REQUIRED_HEADERS[i]] = len(sec_text)

        if len(sec_text) < min_sec_chars:
            raise ValueError(
                f"Section '{REQUIRED_HEADERS[i]}' is too thin ({len(sec_text)} chars < {min_sec_chars} chars)."
            )
        if len(sec_lines) < min_sec_lines:
            raise ValueError(
                f"Section '{REQUIRED_HEADERS[i]}' has too few lines ({len(sec_lines)} < {min_sec_lines})."
            )
        if is_longform and not nested_bullet_re.search(sec_text):
            raise ValueError(
                f"Section '{REQUIRED_HEADERS[i]}' lacks required 2-level indented sub-bullets."
            )
        if is_longform and len(sec_cites) < 2:
            raise ValueError(
                f"Section '{REQUIRED_HEADERS[i]}' has insufficient timestamp citations ({len(sec_cites)} < 2)."
            )

        # Section 3 specific: Quantitative Empirical Data Gate
        if i == 2 and is_longform:
            quant_matches = QUANTITATIVE_PATTERN.findall(sec_text)
            if len(quant_matches) < 5:
                raise ValueError(
                    f"Section 3 (Key Findings) lacks sufficient quantitative metrics ({len(quant_matches)} < 5)."
                )

    # 4. Timestamp Grounding & Temporal Coverage Gate
    cited_raw = CITATION_PATTERN.findall(text)
    if not cited_raw:
        raise ValueError("Report must contain at least 1 timestamp citation ([MM:SS]).")

    min_total_cites = 25 if is_longform else 1
    min_unique_cites = 15 if is_longform else 1
    unique_cited = sorted(set(cited_raw))

    if len(cited_raw) < min_total_cites or len(unique_cited) < min_unique_cites:
        raise ValueError(
            f"Insufficient timestamp citations: total={len(cited_raw)} (min {min_total_cites}), "
            f"unique={len(unique_cited)} (min {min_unique_cites})."
        )

    grounding_ratio = 1.0
    tercile_counts = {"early": 0, "middle": 0, "late": 0}

    if cleaned_text:
        source_anchors_raw = set(CITATION_PATTERN.findall(cleaned_text))
        source_secs = sorted(parse_anchor_seconds(a) for a in source_anchors_raw)
        if source_secs:
            max_sec = source_secs[-1]
            grounded_count = 0
            for c_str in unique_cited:
                c_sec = parse_anchor_seconds(c_str)
                # Exact anchor match or within +/- 60s of a valid source anchor
                if c_str in source_anchors_raw or any(abs(c_sec - s) <= 60.0 for s in source_secs):
                    grounded_count += 1
                if max_sec > 0:
                    if c_sec <= max_sec / 3.0:
                        tercile_counts["early"] += 1
                    elif c_sec <= (2.0 * max_sec) / 3.0:
                        tercile_counts["middle"] += 1
                    elif c_sec <= max_sec + 120.0:
                        tercile_counts["late"] += 1

            grounding_ratio = grounded_count / max(1, len(unique_cited))
            if grounding_ratio < 0.85:
                raise ValueError(
                    f"Timestamp hallucination detected: only {grounding_ratio:.1%} ({grounded_count}/{len(unique_cited)}) "
                    f"of cited timestamps match the source transcript anchors."
                )

            # Require coverage across Early, Middle, and Late terciles for videos >= 15 mins
            if is_longform and max_sec >= 900.0:
                missing_terciles = [k for k, v in tercile_counts.items() if v < 2]
                if missing_terciles:
                    raise ValueError(
                        f"Incomplete temporal coverage across video timeline (tercile counts={tercile_counts}); "
                        f"insufficient citations in: {missing_terciles}."
                    )

    return {
        "char_len": len(text),
        "total_citations": len(cited_raw),
        "unique_citations": len(unique_cited),
        "grounding_ratio": round(grounding_ratio, 4),
        "tercile_counts": tercile_counts,
        "section_lengths": section_Lengths,
    }


def build_judge_ground_truth_sample(cleaned_text: str, max_blocks: int = 18) -> str:
    """Extracts evenly spaced timestamped blocks across the entire transcript for fast LLM Judge verification."""
    blocks = [b.strip() for b in re.split(r"(?=\[\d{1,3}:\d{2}\])", cleaned_text) if b.strip()]
    if not blocks:
        return cleaned_text[:15000]
    if len(blocks) <= max_blocks:
        return "\n".join(b[:700] for b in blocks)
    step = max(1, len(blocks) // max_blocks)
    sampled = [blocks[i][:700] for i in range(0, len(blocks), step)][:max_blocks]
    if blocks[-1][:700] not in sampled:
        sampled.append(blocks[-1][:700])
    return "\n".join(sampled)


def extract_verdict_json(raw_text: str) -> dict | None:
    """Extracts the last valid Judge JSON object containing 'pass' and 'score' from text or reasoning output."""
    if not raw_text:
        return None
    candidates = re.findall(
        r"\{[^{}]*\"pass\"\s*:\s*(?:true|false)[^{}]*(?:\{[^{}]*\}[^{}]*)*\}",
        raw_text,
        re.DOTALL | re.IGNORECASE,
    )
    for cand in reversed(candidates):
        try:
            parsed = json.loads(cand)
            if "pass" in parsed and "score" in parsed:
                return parsed
        except Exception:
            continue
    return None


def audit_with_llm_judge(client: OpenAI, slot_model: str, cleaned_text: str, report_md: str) -> dict:
    """
    Stage 2 Semantic & Fidelity LLM Judge:
    Verifies factual grounding against sampled transcript blocks, methodology depth,
    section role alignment, and English completeness.
    Supports reasoning models that emit JSON in either message.content or message.reasoning.
    """
    sampled_source = build_judge_ground_truth_sample(cleaned_text, max_blocks=10)
    user_payload = (
        "Be concise in your evaluation and output ONLY the final JSON object.\n\n"
        "=== SOURCE TRANSCRIPT ANCHOR EXCERPTS (EARLY / MIDDLE / LATE) ===\n"
        f"{sampled_source}\n\n"
        "=== GENERATED MARKDOWN REPORT TO AUDIT ===\n"
        f"{report_md}"
    )

    verdict = None
    last_raw = ""
    for _judge_attempt in range(2):
        resp = client.chat.completions.create(
            model=slot_model,
            messages=[
                {"role": "system", "content": JUDGE_SYSTEM_PROMPT},
                {"role": "user", "content": user_payload},
            ],
            temperature=0.0,
            max_tokens=16384,
            extra_body={"reasoning": {"effort": "low"}},
            timeout=180.0,
        )
        msg = resp.choices[0].message
        content_text = (msg.content or "").strip()
        reasoning_text = (getattr(msg, "reasoning", None) or "").strip()
        last_raw = content_text or reasoning_text

        verdict = extract_verdict_json(content_text) or extract_verdict_json(reasoning_text)
        if verdict is not None:
            break

    if verdict is None:
        raise ValueError(f"LLM Judge returned non-JSON response: {last_raw[:200]}")

    score = int(verdict.get("score", 0))
    passed = bool(verdict.get("pass", False)) and score >= 80
    defects = verdict.get("defects", [])

    if not passed:
        raise ValueError(f"Stage 2 LLM Judge rejected report (score={score}/100): {defects}")

    return verdict

def resolve_output_path(f_name: str, f_hash: str) -> Path:
    """
    동명 파일 충돌을 방지하기 위해 기존 파일이 다른 해시로 생성된 경우 해시 접미사를 부여합니다.
    """
    candidate = OUTPUT_DIR / f"{f_name}.md"
    if not candidate.exists():
        return candidate

    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute(
            "SELECT file_hash FROM jobs WHERE file_name = ? AND status = 'DONE' AND file_hash != ?",
            (f_name, f_hash)
        ).fetchone()
    if row:
        return OUTPUT_DIR / f"{f_name}_{f_hash[:8]}.md"
    return candidate

# ── 5. 단일 작업 처리기 (Hybrid 2-Stage Audit + Self-Correction Loop) ─────────
def process_single_job(client: OpenAI, slot_model: str, job: tuple) -> bool:
    f_hash, f_name, cleaned_text, retries = job
    out_file = resolve_output_path(f_name, f_hash)
    target_min_len = 7000 if len(cleaned_text) >= 50_000 else 150

    last_audit_error = ""
    current_retries = retries

    while current_retries < MAX_RETRIES:
        user_content = cleaned_text
        if last_audit_error:
            user_content = (
                f"[CRITICAL CORRECTION REQUIRED FROM PREVIOUS ATTEMPT]\n"
                f"Your previous output was rejected by the quality gate for the following reason:\n"
                f"-> {last_audit_error}\n"
                f"Fix this defect completely while obeying all mandatory rules.\n\n"
                f"=== TRANSCRIPT ===\n{cleaned_text}"
            )

        try:
            # 1) Generation LLM Call
            resp = client.chat.completions.create(
                model=slot_model,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": user_content}
                ],
                temperature=0.15,
                max_tokens=MAX_OUTPUT_TOKENS,
                timeout=300.0
            )

            choice = resp.choices[0]
            content = choice.message.content

            # 2) Stage 1: Deterministic Python Structural, Mathematical & Grounding Audit
            stage1_stats = validate_markdown_output(
                content,
                cleaned_text=cleaned_text,
                finish_reason=choice.finish_reason,
                min_length=target_min_len,
            )
            print(
                f"  ✅ [Stage 1 Deterministic Gate PASS] len={stage1_stats['char_len']:,} chars, "
                f"cites={stage1_stats['total_citations']} (unique={stage1_stats['unique_citations']}, "
                f"grounded={stage1_stats['grounding_ratio']:.1%}), terciles={stage1_stats['tercile_counts']}"
            )

            # 3) Stage 2: LLM Semantic & Fidelity Judge Audit
            verdict = audit_with_llm_judge(client, slot_model, cleaned_text, content)
            print(
                f"  ✅ [Stage 2 LLM Semantic Judge PASS] score={verdict.get('score')}/100, "
                f"breakdown={verdict.get('scores')}"
            )

            out_file.write_text(content, encoding="utf-8")

            with sqlite3.connect(DB_PATH) as conn:
                conn.execute(
                    "UPDATE jobs SET retries = ?, status = 'DONE', updated_at = ? WHERE file_hash = ?",
                    (current_retries, time.time(), f_hash)
                )
            print(f"[성공] {f_name} -> {out_file.name}")
            return True

        except Exception as e:
            last_audit_error = str(e)
            current_retries += 1
            new_status = "FAILED" if current_retries >= MAX_RETRIES else "PENDING"
            print(f"[실패/자율교정] {f_name} (시도 {current_retries}/{MAX_RETRIES}, 상태={new_status}): {e}")
            with sqlite3.connect(DB_PATH) as conn:
                conn.execute(
                    "UPDATE jobs SET retries = ?, status = ?, updated_at = ? WHERE file_hash = ?",
                    (current_retries, new_status, time.time(), f_hash)
                )
            if current_retries < MAX_RETRIES:
                time.sleep(1.5)
        finally:
            # 워커 스레드 내에서 1.0초 안전 지터 유지 (Least-In-Flight 재정렬 보장)
            time.sleep(1.0)

    return False

# ── 6. 메인 배치 파이프라인 ───────────────────────────────────────────────────
def run_pipeline(
    concurrency: int = 2,
    domain: str = "medical",
    sub_domain: str = "general",
    channel: str = "",
    index_after: bool = False,
):
    """
    jpro-router가 10개 이상의 계정을 관리하므로,
    Subflow는 굳이 35초씩 기다리지 않고 2개 스레드 동시 처리 및 최소 대기로 100개를 고속 완주합니다.
    구조화 완료 후 index_after=True인 경우 OmniLake LanceDB 인덱서(indexer.py)를 자동 기동합니다.
    """
    client, slot_model = resolve_router_client()
    conn = init_db()

    # 1. 신규 파일 스캔 및 큐 등록
    for file_path in sorted(INPUT_DIR.glob("*.*")):
        if file_path.suffix.lower() not in {".vtt", ".srt", ".txt"}:
            continue
        file_bytes = file_path.read_bytes()
        f_hash = hashlib.sha256(file_bytes).hexdigest()

        cur = conn.execute("SELECT status FROM jobs WHERE file_hash = ?", (f_hash,))
        if cur.fetchone():
            continue

        vid_match = re.search(r"\[([a-zA-Z0-9_-]{11})\]", file_path.stem)
        video_id = vid_match.group(1) if vid_match else ""

        cleaned = clean_transcript(file_path, video_id)
        conn.execute(
            """INSERT INTO jobs (
                file_hash, file_name, video_id, cleaned_text, updated_at,
                domain, sub_domain, channel_name, video_title, index_status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'PENDING')""",
            (f_hash, file_path.stem, video_id, cleaned, time.time(), domain, sub_domain, channel, file_path.stem)
        )
    conn.commit()

    # 2. 대기 목록 가져오기
    jobs = conn.execute(
        "SELECT file_hash, file_name, cleaned_text, retries FROM jobs WHERE status = 'PENDING' AND retries < ? LIMIT 100",
        (MAX_RETRIES,)
    ).fetchall()

    if not jobs:
        print("[완료] 처리할 대기 스크립트가 없습니다.")
    else:
        print(f"[시작] 총 {len(jobs)}개 스크립트 구조화 시작 (동시성: {concurrency}, 슬롯: {slot_model})")

        # 3. 고속 배치 실행
        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = {executor.submit(process_single_job, client, slot_model, job): job for job in jobs}
            for future in as_completed(futures):
                _ = future.result()

    # 4. OmniLake 인덱싱 (index_after 옵션 활성화 시)
    if index_after:
        print("\n[OmniLake] 구조화 완료 문서 및 자막의 레이크하우스(LanceDB) 인덱싱 기동...")
        try:
            from indexer import OmniLakeIndexer
            lake_indexer = OmniLakeIndexer()

            pending_indices = conn.execute("""
                SELECT file_hash, file_name, video_id, domain, sub_domain, channel_name, video_title
                FROM jobs WHERE status = 'DONE' AND index_status != 'INDEXED'
            """).fetchall()

            for item in pending_indices:
                f_hash, f_name, vid, dom, sub_dom, chan, v_title = item
                out_file = resolve_output_path(f_name, f_hash)
                vtt_candidates = list(INPUT_DIR.glob(f"*{vid}*.vtt"))
                vtt_file = vtt_candidates[0] if vtt_candidates else None

                stats = lake_indexer.index_video(
                    video_id=vid,
                    title=v_title or f_name,
                    channel=chan or "Unknown Channel",
                    domain=dom or domain,
                    sub_domain=sub_dom or sub_domain,
                    vtt_path=vtt_file,
                    md_path=out_file if out_file.exists() else None,
                    mode="deep"
                )
                conn.execute(
                    "UPDATE jobs SET index_status = 'INDEXED', chunks_indexed = ?, updated_at = ? WHERE file_hash = ?",
                    (stats.get("total_chunks", 0), time.time(), f_hash)
                )
                conn.commit()
        except Exception as e:
            print(f"[OmniLake 인덱싱 오류]: {e}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Subflow Structuring Pipeline & OmniLake Indexer")
    parser.add_argument("--concurrency", "-c", type=int, default=2, help="동시 LLM 워커 수 (기본값: 2)")
    parser.add_argument("--domain", "-d", type=str, default="medical", choices=["medical"], help="대상 지식 도메인 (기본값: medical / MitoLake)")
    parser.add_argument("--sub-domain", "-s", type=str, default="mitochondria", help="세부 도메인 (예: mitochondria, anti_aging, brain_health, skin_aging)")
    parser.add_argument("--channel", type=str, default="", help="유튜브 채널명")
    parser.add_argument("--index", action="store_true", help="구조화 완료 후 OmniLake LanceDB 인덱싱까지 자동 실행")

    cli_args = parser.parse_args()
    run_pipeline(
        concurrency=cli_args.concurrency,
        domain=cli_args.domain,
        sub_domain=cli_args.sub_domain,
        channel=cli_args.channel,
        index_after=cli_args.index,
    )

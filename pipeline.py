import os
import re
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

# ── 2. 프롬프트 규격 ──────────────────────────────────────────────────────────
SYSTEM_PROMPT = """당신은 학술 세미나, 심층 인터뷰, 기술/비즈니스 롱폼 영상을 원문 손실 없이 체계적으로 해부하여 문서화하는 수석 연구원입니다.
제공된 스크립트에서 단순 인사말, 추임새, 스폰서 광고만 제거하고, 영상에 등장하는 **모든 핵심 프레임워크, 세부 방법론, 실무 노하우, 에피소드, 수치 데이터, 프롬프트/공식**을 빠짐없이 포착하여 아래 규격의 고밀도 심층 Markdown 리포트로 작성하세요.

[필수 준수 규칙 - 위반 시 출력 거부됨]
1. 단문 개조식 요약을 금지합니다. 각 항목은 **상위 핵심 명제 + 하위 세부 설명(메커니즘, 이유, 구체적 예시, 실행 공식)**의 2단 계층 구조(Nested Bullets)로 깊이 있게 서술하십시오.
2. 방법론이나 프레임워크를 설명할 때는 명칭의 유래/정의(예: 왜 3-2-1인지 각 숫자의 의미), 단계별 프로토콜, 부수적으로 언급된 실무 팁(예: 프로필 Bio 설계 공식, 보조 AI 툴 활용 에피소드 등)까지 생략 없이 모두 포함하십시오.
3. 모든 핵심 주장, 방법론, 실험 수치, 사례 끝에는 반드시 원문에 표시된 타임스탬프(예: [14:00], [75:00])를 인용 표기하십시오. (최소 25회 이상 전 구간에 걸쳐 촘촘히 인용)
4. 원문에 근거가 없는 추론이나 수치는 절대 지어내지 마십시오.
5. 아래 5개 섹션 헤더를 **반드시 모두 포함**하여 전체 분량 **6,000자~8,000자(약 1,200~1,600 어절) 수준**의 심층 리포트로 작성하십시오.

=== 출력 규격 (반드시 이 헤더들을 정확히 사용) ===

# [제목: 영상의 핵심 주제 및 대상 연구/기술/프레임워크명]

### 1. 핵심 질문 및 문제 의식
- 해결하고자 하는 근본 문제, 기존 통념의 구조적 오류, 화자의 배경 및 핵심 통찰을 4~6개 대항목(각 항목당 하위 불릿 2~3개)으로 상세히 서술 ([MM:SS] 인용 필수)

### 2. 제안 방법론 및 프레임워크 (Methodology)
- 영상에 등장한 **모든 주요 프레임워크와 부수적 실무 전술**을 하나도 빠뜨리지 말고 각각 독립된 대항목으로 분류한 뒤, 각 항목 아래에 ① 개념 정의 및 구조 ② 단계별 작동 메커니즘 ③ 실제 적용 예시/프롬프트/수치를 하위 불릿으로 상세히 해부 ([MM:SS] 인용 필수)

### 3. 주요 연구 결과 및 검증 데이터 (Key Findings)
- 영상 전반에 언급된 모든 정량적 성과, 전환율 공식, A/B 비교 사례, 기존 방식 대비 효율 비교, AI/데이터 분석이 도출한 세부 패턴 목록(예: 7가지 패턴 각각의 상세 내용) 등을 누락 없이 최소 6~10개 대항목으로 정밀 기록 ([MM:SS] 인용 필수)

### 4. 한계점 및 주의사항 (Limitations)
- 화자가 경고한 예외 조건, 잘못된 적용 시 발생하는 부작용(알고리즘 오염, 파생 콘텐츠화 등), 도구의 한계 및 이를 극복하기 위한 우회 조건을 최소 5~7개 대항목으로 구체적 맥락과 함께 서술 ([MM:SS] 인용 필수)

### 5. 현업 적용/실천 가이드 (Actionable Takeaways)
- 영상을 직접 보지 않고 이 문서만 읽고도 100% 동일하게 복제 실행할 수 있도록, 사전 준비부터 주간 루틴·측정 지표·도구 세팅·확장 로드맵까지 최소 6~8단계(각 단계별 하위 액션 아이템 포함)로 상세히 작성 ([MM:SS] 인용 필수)

=== 중요 ===
위 5개 헤더(### 1. ~ ### 5.)는 **반드시 그대로 출력**하십시오.
초압축 요약을 지양하고, 원문의 디테일과 사례가 살아있도록 기존 요약본 대비 **2배 이상의 정보 밀도와 분량(6,000자 이상)**으로 풍부하게 서술하십시오.
마지막 5번 섹션의 끝 문장까지 절대 중간에 끊기지 않도록 완결된 문장으로 마무리하십시오."""

MAX_RETRIES = 5
MAX_OUTPUT_TOKENS = 16384
SAFE_CHAR_LIMIT = 400_000  # 약 100K~120K 토큰 안전 상한 (하위 Fallback 모델 컨텍스트 보호)
REQUIRED_HEADERS = ("### 1.", "### 2.", "### 3.", "### 4.", "### 5.")
CITATION_PATTERN = re.compile(r"\[\d{1,3}:\d{2}(?::\d{2})?\]")
TRUNCATED_END_CHARS = ("·", ",", ":", ";", "-", "*", "(", "[", "/")

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

def validate_markdown_output(content: str, finish_reason: str | None = None, min_length: int = 150) -> None:
    """
    생성된 마크다운 리포트의 최소 길이, 필수 5단 섹션 헤더, 타임스탬프 인용 여부 및 문장 완결성을 검증합니다.
    """
    if finish_reason == "length":
        raise ValueError("출력 토큰 상한 도달로 인해 리포트가 중간에 절단되었습니다 (finish_reason=length).")

    text = (content or "").strip()
    if len(text) < min_length:
        raise ValueError(f"생성된 노트가 목표 밀도보다 짧습니다 ({len(text)}자 < {min_length}자).")

    missing_headers = [h for h in REQUIRED_HEADERS if h not in text]
    if missing_headers:
        raise ValueError(f"필수 섹션 헤더가 누락되었습니다: {missing_headers}")

    if not CITATION_PATTERN.search(text):
        raise ValueError("본문 내 타임스탬프 인용([MM:SS])이 최소 1회 이상 포함되어야 합니다.")

    # 마지막 5번 섹션 이후 최소 분량 및 문장 절단 여부 검증
    sec5_idx = text.rfind("### 5.")
    sec5_body = text[sec5_idx:].splitlines()[1:]
    non_empty_sec5 = [line.strip() for line in sec5_body if line.strip()]
    if len(non_empty_sec5) < 2:
        raise ValueError("5번 섹션(현업 적용/실천 가이드) 본문이 부족하거나 누락되었습니다.")

    if text.endswith(TRUNCATED_END_CHARS):
        raise ValueError(f"리포트 마지막 문장이 비정상적으로 절단되었습니다 (끝 문자: '{text[-1]}').")

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

# ── 5. 단일 작업 처리기 ─────────────────────────────────────────────────────────
def process_single_job(client: OpenAI, slot_model: str, job: tuple) -> bool:
    f_hash, f_name, cleaned_text, retries = job
    out_file = resolve_output_path(f_name, f_hash)

    try:
        # jpro-router에 단일 요청 전송 (프록시가 10+개 키 로테이션 및 폴백 자동 집행)
        resp = client.chat.completions.create(
            model=slot_model,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": cleaned_text}
            ],
            temperature=0.15,
            max_tokens=MAX_OUTPUT_TOKENS,
            timeout=300.0
        )

        choice = resp.choices[0]
        content = choice.message.content
        target_min_len = 5000 if len(cleaned_text) >= 50000 else 150
        validate_markdown_output(content, finish_reason=choice.finish_reason, min_length=target_min_len)

        out_file.write_text(content, encoding="utf-8")

        with sqlite3.connect(DB_PATH) as conn:
            conn.execute(
                "UPDATE jobs SET status = 'DONE', updated_at = ? WHERE file_hash = ?",
                (time.time(), f_hash)
            )
        print(f"[성공] {f_name} -> {out_file.name}")
        return True

    except Exception as e:
        new_retries = retries + 1
        new_status = "FAILED" if new_retries >= MAX_RETRIES else "PENDING"
        print(f"[실패] {f_name} (시도 {new_retries}/{MAX_RETRIES}, 상태={new_status}): {e}")
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute(
                "UPDATE jobs SET retries = ?, status = ?, updated_at = ? WHERE file_hash = ?",
                (new_retries, new_status, time.time(), f_hash)
            )
        return False
    finally:
        # 워커 스레드 내에서 1.0초 안전 지터 유지 (Least-In-Flight 재정렬 보장)
        time.sleep(1.0)

# ── 6. 메인 배치 파이프라인 ───────────────────────────────────────────────────
def run_pipeline(concurrency: int = 2):
    """
    jpro-router가 10개 이상의 계정을 관리하므로,
    Subflow는 굳이 35초씩 기다리지 않고 2개 스레드 동시 처리 및 최소 대기로 100개를 고속 완주합니다.
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
            "INSERT INTO jobs (file_hash, file_name, video_id, cleaned_text, updated_at) VALUES (?, ?, ?, ?, ?)",
            (f_hash, file_path.stem, video_id, cleaned, time.time())
        )
    conn.commit()

    # 2. 대기 목록 가져오기
    jobs = conn.execute(
        "SELECT file_hash, file_name, cleaned_text, retries FROM jobs WHERE status = 'PENDING' AND retries < ? LIMIT 100",
        (MAX_RETRIES,)
    ).fetchall()

    if not jobs:
        print("[완료] 처리할 대기 스크립트가 없습니다.")
        return

    print(f"[시작] 총 {len(jobs)}개 스크립트 구조화 시작 (동시성: {concurrency}, 슬롯: {slot_model})")

    # 3. 고속 배치 실행
    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = {executor.submit(process_single_job, client, slot_model, job): job for job in jobs}
        for future in as_completed(futures):
            _ = future.result()

if __name__ == "__main__":
    run_pipeline(concurrency=2)

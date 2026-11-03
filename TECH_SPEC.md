# Subflow: Technical Specification Document

**Document Version:** 2.0.0  
**Status:** Approved / Architecture Finalized  
**Author:** AI Architecture Team  
**Date:** 2026-11-03  
**Target Environment:** macOS (Darwin arm64) + Local `jpro-router` (`http://127.0.0.1:4001`)

---

## 1. Executive Summary & Mission

**Subflow**는 유튜브 롱폼(1~3시간 이상 분량의 학술 세미나, 기술 팟캐스트, 심층 인터뷰 등)의 자막 데이터를 비용 **$0.00**으로 정밀 구조화된 마크다운 연구 리포트로 변환하는 고성능 로컬 배치 파이프라인이다.

### 1.1 핵심 미션 및 설계 원칙
1. **$0 운영 비용의 엄격한 달성**: 상용 유료 API(GPT-4o, Claude 3.5 Sonnet 원격 결제 등)를 일체 사용하지 않으며, 100만(1M) 토큰 컨텍스트를 지원하는 오픈 모델 풀을 로컬 프록시를 통해 활용한다.
2. **`jpro-router` 절대 불변 원칙 (Infrastructure Black-box)**: Mac 로컬에서 구동 중인 AI 프록시 `jpro-router`(`http://127.0.0.1:4001`)의 소스코드는 단 1바이트도 수정하지 않는다. 10개 이상의 OpenRouter 다중 계정 로테이션, Least-In-Flight 스케줄링, 429 감지 및 계층형 Fallback은 모두 `jpro-router`가 담당하며, Subflow는 표준 OpenAI 호환 클라이언트로만 기능한다.
3. **단일 패스(One-Shot) 파이프라인**: 청크 분할, 다단계 임베딩, 다중 LLM 질의 등 불필요한 네트워크 I/O를 배제하고, 로컬 결정론적 전처리 후 1M 컨텍스트 LLM에 **단 1회 호출(Single-Pass)**로 완결한다.
4. **결정론적 환각 억제 (Deterministic Grounding)**: 별도의 검증 모델 없이 원문 스크립트에 60초 간격의 타임스탬프 앵커(`[MM:SS]`)를 삽입하고, LLM이 모든 주장에 대해 원문 타임스탬프를 강제 인용하도록 규격화한다.
5. **일일 100개 무중단 처리 (Resilient & Resumable)**: SQLite 상태 머신을 기반으로 장애 발생, 네트워크 단절, 프로세스 강제 종료 시에도 멈춘 지점부터 즉시 재개 가능한 멱등성을 보장한다.

---

## 2. System Architecture & Topology

```
+-----------------------------------------------------------------------------------+
|                            Subflow Processing Pipeline                            |
+-----------------------------------------------------------------------------------+
                                         │
        [ 1. Ingestion ]                 ▼
  transcripts/*.vtt, *.srt ──► [ SHA-256 Hash Audit & SQLite State Queue ]
                                         │
        [ 2. Preprocessing ]             ▼
                       ┌──► [ SponsorBlock API (Skip Sponsor/Intro Intervals) ]
                       └──► [ VTT Rolling Overlap Removal & Text Normalization ]
                       └──► [ Inject 60s Interval [MM:SS] Anchors ]
                                         │
        [ 3. Dispatch ]                  ▼
                        [ Zero-Config Discovery: GET /connect ]
                                         │
                                         ▼ (HTTP POST /openai/v1/chat/completions)
+-----------------------------------------------------------------------------------+
|                     jpro-router Local AI Proxy (Port 4001)                        |
|                                                                                   |
|  - Auth: Local Token Verification (LOCAL_PROXY_AUTH_TOKEN)                        |
|  - Virtual Socket Mapping: gpt-6.1-sol                                            |
|  - Upstream Key Pool: 10+ Accounts (OPENROUTER_API_KEY_1..50)                     |
|  - Key Scheduler: Least-In-Flight / Sequential Rotation                           |
|  - Fallback Chain:                                                                |
|      nvidia/nemotron-3-ultra-550b-a55b:free (1M Context)                          |
|        └──► nvidia/nemotron-3.5-super-vl:free                                     |
|              └──► nvidia/nemotron-3.5-lightning:free                              |
|                    └──► google/gemma-4-31b-it:free                                |
|  - Rate-Limit Resilience: Tier 3 Saturated Bypass & Instant Account Shifting      |
+-----------------------------------------------------------------------------------+
                                         │
        [ 4. Extraction ]                ▼
                       [ Markdown Extraction & Header/Citation Audit ]
                                         │
        [ 5. Persistence ]               ▼
                       [ Write structured_notes/*.md & SQLite Status = DONE ]
```

---

## 3. Detailed Component Specifications

### 3.1 Tier 1: Local Deterministic Preprocessing (`clean_transcript`)

유튜브 자동 생성 자막(VTT/SRT)은 시간 경과에 따라 이전 문장이 다음 줄에 반복되는 **롤링 캡션(Rolling Caption)** 문제를 필연적으로 동반한다. 단순 정규식으로 타임스탬프만 삭제할 경우 동일 어구가 2~3회 반복되어 토큰이 100% 이상 낭비된다.

#### 3.1.1 SponsorBlock API 연동
- **Endpoint**: `https://sponsor.ajay.app/api/skipSegments?videoID={video_id}&categories=["sponsor","intro","outro","selfpromo","interaction"]`
- **목적**: 영상에 포함된 광고, 인트로, 채널 구독 유도 구간의 초(second) 단위 시작/종료 인터벌 리스트를 수신하여 자막 파싱 단계에서 원천 배제.
- **예외 처리**: 타임아웃 3초 설정. 네트워크 장애 또는 DB 미등록 영상인 경우 빈 리스트 `[]`를 반환하여 본문 파싱에 영향이 없도록 설계.

#### 3.1.2 롤링 자막 중복 병합 알고리즘 (Suffix-Prefix Sliding Overlap)
- 정규식 `(\d{1,2}:\d{2}(?::\d{2})?[.,]\d{1,3})\s*-->\s*(\d{1,2}:\d{2}(?::\d{2})?[.,]\d{1,3})`으로 타임코드 블록 파싱.
- 인접한 두 자막 라인 $L_{i-1}$과 $L_i$ 간의 단순 Prefix 포함뿐만 아니라, 2줄 이상 롤링 캡션에서 발생하는 **Suffix-Prefix 최장 공통 단어열($k$)**을 탐색하여 제거(`extract_non_overlapping_delta`):
  $$k^* = \max \{ k \mid \text{words}(L_{i-1})[-k:] = \text{words}(L_i)[:k] \}, \quad \Delta L_i = \text{words}(L_i)[k^*:]$$
- 이전 텍스트와의 겹침을 실시간 절삭하여 순수 신규 발언($\Delta L_i$)만 누적.
- **컨텍스트 안전 상한 (`SAFE_CHAR_LIMIT = 400,000`)**: 하위 Fallback 모델(예: 128K 컨텍스트 모델)로 전이될 때 `400 Bad Request (Context Length Exceeded)`가 발생하지 않도록 정제된 텍스트 본문을 최대 400,000자(약 100K~120K 토큰) 이내로 안전 절삭.

#### 3.1.3 60초 단위 타임스탬프 앵커 삽입
- 모든 라인마다 타임스탬프를 남기면 토큰 오버헤드가 발생하므로, 누적 시간 $t - t_{last} \ge 60.0\text{s}$일 때만 앵커 `\n[MM:SS] `를 삽입(60분 이상 구간은 `[75:00]` 등 누적 분 표기 허용).
- 이를 통해 2시간 영상 기준 약 120개의 시간 이정표만 생성하여 토큰을 절약하면서도 정밀한 위치 추적성을 확보.

---

### 3.2 Tier 2: Zero-Config `jpro-router` Integration

Subflow는 환경설정에 API 키나 프록시 주소를 하드코딩하지 않고, `jpro-router`의 공개 제어면 엔드포인트를 통해 런타임에 동적으로 바인딩한다.

#### 3.2.1 1초 자동 디스커버리 프로토콜
- **요청**: `GET http://127.0.0.1:4001/connect` (무인증, CORS 안전)
- **응답 수신 필드**:
  ```json
  {
    "endpoints": {
      "openai": {
        "base_url": "http://127.0.0.1:4001/openai/v1",
        "default_model": "gpt-6.1-sol"
      }
    },
    "auth_token": "..."
  }
  ```
- **초기화 계약**:
  ```python
  client = OpenAI(base_url=base_url, api_key=auth_token or "local-token")
  ```

#### 3.2.2 가상 소켓(Virtual Socket) 규약
- 클라이언트는 업스트림 모델명(예: `nvidia/nemotron-3-ultra-550b-a55b:free`)을 직접 명시하지 않고, 반드시 **`gpt-6.1-sol`**을 모델 파라미터로 지정한다.
- `jpro-router`의 내부 라우팅 테이블이 이를 100만 컨텍스트 무료 모델 풀에 매핑하며, 업스트림 장애 발생 시 백엔드 전환을 클라이언트 모르게 투명하게 처리한다.

---

### 3.3 Tier 3: State Machine & Idempotency Engine (`subflow_state.db`)

모든 작업은 로컬 SQLite 데이터베이스에 영속화되어 원자성(Atomicity)과 재개 가능성(Resumability)을 보장한다.

#### 3.3.1 스키마 명세
```sql
CREATE TABLE IF NOT EXISTS jobs (
    file_hash TEXT PRIMARY KEY,   -- SHA-256(raw_file_bytes)
    file_name TEXT NOT NULL,       -- 원본 파일 기본명 (Stem)
    video_id TEXT,                 -- 유튜브 11자리 비디오 식별자
    cleaned_text TEXT,             -- 전처리 완료된 텍스트 본문
    status TEXT DEFAULT 'PENDING', -- PENDING, DONE, FAILED
    retries INTEGER DEFAULT 0,     -- 재시도 누적 횟수 (최대 5회)
    updated_at REAL                -- 최종 상태 변경 유닉스 타임스탬프
);

CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);
```

#### 3.3.2 멱등성 보장 수명주기 (State Lifecycle)
1. **스캔 및 적재**: 신규 파일 발견 시 SHA-256 해시를 계산하여 `jobs` 테이블에 미등록된 항목만 `PENDING`으로 등록.
2. **배치 인출**: `SELECT * FROM jobs WHERE status = 'PENDING' AND retries < 5 LIMIT 100`으로 대상 추출.
3. **출력 구조 감사 (`validate_markdown_output`)**:
   - 본문 길이 `< 150`자 여부 확인
   - 5대 필수 섹션 헤더(`### 1.` ~ `### 5.`) 전수 포함 여부 검증
   - 정규식 `\[\d{1,3}:\d{2}(?::\d{2})?\]`를 통한 타임스탬프 인용 최소 1회 이상 포함 여부 검증
4. **완료 트랜잭션 및 파일명 충돌 방지 (`resolve_output_path`)**:
   - 동명(`file_name`)이지만 파일 해시(`file_hash`)가 다른 기존 완료본이 존재할 경우 `{file_name}_{file_hash[:8]}.md`로 저장하여 덮어쓰기 방지.
   - 마크다운 파일 디스크 저장 완료 후 `status = 'DONE'` 업데이트.
5. **장애 격리**: 예외 발생 시 `retries = retries + 1` 증가. 5회(`MAX_RETRIES`) 도달 시 `status = 'FAILED'`로 명시적 상태 전이 후 영구 격리.

---

### 3.4 Tier 4: Prompt Engineering & Grounding Contract

추출 결과물의 학술적 깊이와 일관성을 보장하기 위해 엄격한 마크다운 헤더 구조와 인용 규칙을 강제한다.

#### 3.4.1 시스템 프롬프트 규격
- **역할 정의**: 학술 및 기술 전문 분석 연구원.
- **노이즈 필터링**: 인사말, 사담, 채널 홍보, 스폰서 광고 문구에 대한 무조건적 무시(Ignore) 지시.
- **그라운딩 제약 조건 (Critical Constraint)**:
  > "모든 핵심 주장, 메커니즘, 실험 수치, 통계 데이터의 끝에는 반드시 원문에 표시된 `[MM:SS]` 타임스탬프를 명기하라. 원문에 타임스탬프가 없는 사실은 절대 작성하지 말라."

#### 3.4.2 출력 표준 규격 (5단 구조)
```markdown
# [영상의 핵심 주제 및 연구/기술명]

### 1. 핵심 질문 및 문제 의식
- 해결하고자 하는 문제 정의 및 배경

### 2. 제안 방법론 및 프레임워크 (Methodology)
- 구체적 절차, 아키텍처, 프로토콜 규격 ([MM:SS] 인용 필수)

### 3. 주요 연구 결과 및 검증 데이터 (Key Findings)
- 실험 수치, 통계적 지표, 벤치마크 결과 ([MM:SS] 인용 필수)

### 4. 한계점 및 주의사항 (Limitations)
- 화자가 언급한 제약 사항, 부작용, 엣지 케이스

### 5. 현업 적용/실천 가이드 (Actionable Takeaways)
- 단계별 실행 체크리스트 및 실무 권장 사항
```

---

## 4. Performance, Concurrency & SLA

`jpro-router`가 최소 10개 이상의 OpenRouter 계정을 회전시키므로, 단일 계정 한계(20 RPM)에 묶여 35초씩 대기할 필요가 없다.

### 4.1 토큰 및 용량 프로파일
- **영상당 평균 입력 스크립트**: 30,000 ~ 50,000 토큰 (약 120KB ~ 200KB 텍스트)
- **영상당 평균 출력 리포트**: 1,500 ~ 2,500 토큰 (약 6KB ~ 10KB 마크다운)
- **단일 요청 처리 시간**: 8초 ~ 15초 (Nemotron Ultra 기준)

### 4.2 동시성 및 배치 속도 사양
- **작업자 풀 (Worker Pool)**: `ThreadPoolExecutor(max_workers=2)`
- **워커 내 안전 지터 (Worker-level Inter-task Jitter)**: 각 워커 스레드의 `process_single_job` 종료 시점(`finally` 블록)마다 `1.0초` 대기하여 풀 내 계정 간 Least-In-Flight 재정렬 보장
- **일일 100개 배치 처리 시간**:
  $$\text{총 소요 시간} = \frac{100 \text{ jobs} \times (12\text{s} + 1.0\text{s})}{2 \text{ workers}} = 650\text{s} \approx \mathbf{10\text{분 } 50\text{초}}$$
- 단일 계정 직렬 실행 시 75분이 걸리던 파이프라인을 **11분 내외**로 85% 이상 단축.

---

## 5. Failure Modes & Operational Resilience

| 잠재적 장애 상황 | 감지 메커니즘 | 대응 및 복구 동작 (Resolution) |
| :--- | :--- | :--- |
| **SponsorBlock 네트워크 단절/지연** | `requests.get` 타임아웃 3초 초과 | 빈 인터벌 반환 후 원문 전처리 계속 진행 (Fail-Open) |
| **jpro-router 서버 미기동** | `GET /connect` 연결 거부 (Connection Refused) | 셸 안내 로그 출력 후 기본 로컬 환경변수로 안전 폴백 시도 |
| **특정 무료 모델 일시 포화 (Tier 3)** | HTTP 429 / 503 수신 | `jpro-router`가 `Super VL` ➔ `Lightning` ➔ `Gemma 4`로 자동 전이 |
| **하위 Fallback 모델 컨텍스트 초과 위험** | 입력 스크립트 > 400,000자 | `SAFE_CHAR_LIMIT`(400,000자)으로 선제 절삭하여 128K 컨텍스트 모델 호환성 보장 |
| **특정 OpenRouter 계정 일일 한도 소진** | HTTP 429 (Daily Limit) 수신 | `jpro-router`가 잔여 한도가 남은 다음 계정으로 0초 만에 교체 |
| **비정상 단축 출력 / 헤더·인용 누락** | 길이 < 150자, 필수 5단 헤더 누락, `[MM:SS]` 인용 미포함 | `validate_markdown_output`에서 `ValueError` 발생 ➔ `retries` 증가 (5회 도달 시 `FAILED` 전환) |
| **동명 자막 파일 충돌 (다른 영상)** | 동일 `file_name`에 상이한 `file_hash` 완료본 존재 | `resolve_output_path`가 `{file_name}_{hash[:8]}.md`로 자동 분기 저장 |
| **프로세스 비정상 종료 (SIGKILL 등)** | OS 재부팅 또는 강제 종료 | 재기동 시 `PENDING` 상태인 파일부터 즉시 재개 (Zero Data Loss) |

---

## 6. Directory Structure & File Manifest

Subflow 프로젝트 루트(`/Users/jameskim/Projects/Subflow`)의 파일 구성 및 역할:

```
/Users/jameskim/Projects/Subflow
├── pipeline.py            # [Core] 무인증 디스커버리, VTT 정제, 고속 2스레드 배치 워커
├── requirements.txt       # [Dependencies] openai, requests
├── subflow_state.db       # [State Store] SQLite 작업 큐 및 해시 인덱스 (실행 시 자동 생성)
├── transcripts/           # [Input] 로컬 자막 원본 파일 (.vtt, .srt, .txt) 보관 디렉토리
├── structured_notes/      # [Output] 완성된 구조화 마크다운 리포트 저장 디렉토리
├── README.md              # [Docs] 실행 가이드 및 핵심 원칙 요약
└── TECH_SPEC.md           # [Specs] 본 테크스펙 문서
```

---

## 7. Verification & Operational Checklist

1. **jpro-router 상태 확인**:
   ```bash
   curl -s http://127.0.0.1:4001/connect | jq .status
   # "running" 확인
   ```
2. **Subflow 자막 투입**:
   - `yt-dlp` 등을 통해 `transcripts/` 폴더에 자막을 다운로드한다.
   - 파일명에 유튜브 비디오 ID가 포함되면 SponsorBlock 연동이 자동 활성화된다. (예: `Lecture_Title_[dQw4w9WgXcQ].vtt`)
3. **배치 파이프라인 기동**:
   ```bash
   python pipeline.py
   ```
4. **산출물 확인**:
   - `structured_notes/*.md` 파일 생성 여부 확인.
   - 마크다운 본문 내 `[MM:SS]` 타임스탬프 인용 표기 정상 반영 확인.

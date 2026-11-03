# Subflow (jpro-router Dedicated YouTube Transcript Structuring Pipeline)

`jpro-router` 로컬 프록시(`http://127.0.0.1:4001`) 전용 초저비용($0.00) 유튜브 롱폼 스크립트 수집 및 구조화 파이프라인입니다.

## 핵심 설계 원칙
- **2-Tier 안티봇 수집기 (`collector.py`)**: `youtube-transcript-api` (초고속/경량)를 기본으로 사용하고, 429나 봇 감지 발생 시 `yt-dlp --cookies-from-browser chrome`으로 자동 전이하여 Rate Limit을 완벽히 방어합니다.
- **jpro-router 불변 원칙**: 로컬 프록시 코드는 일체 수정하지 않습니다. 10개 이상의 OpenRouter 계정 로테이션, Least-In-Flight 스케줄링, 429 감지 및 Fallback 체인은 로컬 프록시가 100% 전담합니다.
- **Zero-Config 연결**: 파이프라인 기동 시 `http://127.0.0.1:4001/connect`를 호출하여 로컬 인증 토큰과 가상 슬롯(`gpt-6.1-sol`)을 자동 인식합니다.
- **단일 패스(One-Shot) 구조화**: 로컬에서 SponsorBlock 및 VTT 롤링 중복만 제거한 뒤, 100만 컨텍스트 무료 슬롯에 단 1회 요청하여 정밀 마크다운 노트를 생성합니다.

## 실행 방법

### 1. 의존성 설치
```bash
pip install -r requirements.txt
```

### 2. 유튜브 자막 수집 (신규 2-Tier Collector)
```bash
# 단일 영상 수집
python collector.py "https://www.youtube.com/watch?v=lZc30sUfx64"

# 재생목록 전체 수집
python collector.py "https://www.youtube.com/playlist?list=PL..."

# 텍스트 파일(URL 목록) 수집
python collector.py --file urls.txt

# 수집 완료 후 즉시 LLM 구조화 파이프라인까지 원스톱 실행
python collector.py "https://youtu.be/lZc30sUfx64" --run-pipeline
```

### 3. 구조화 파이프라인 단독 실행 (수동 투입 자막 일괄 처리)
```bash
python pipeline.py
```

생성된 구조화 문서는 `structured_notes/*.md`에 저장되며, 처리 이력은 `subflow_state.db`에 자동 기록되어 재실행 시 중복 작업을 건너뜁니다.

# Subflow (jpro-router Dedicated YouTube Transcript Structuring Pipeline)

`jpro-router` 로컬 프록시(`http://127.0.0.1:4001`) 전용 초저비용 유튜브 롱폼 스크립트 구조화 파이프라인입니다.

## 핵심 설계 원칙
- **jpro-router 불변 원칙**: `jpro-router`의 코드는 일체 수정하지 않습니다. 10개 이상의 OpenRouter 계정 로테이션, Least-In-Flight 스케줄링, 429 감지 및 Fallback 체인은 로컬 프록시가 100% 전담합니다.
- **Zero-Config 연결**: 파이프라인 기동 시 `http://127.0.0.1:4001/connect`를 호출하여 로컬 인증 토큰과 가상 슬롯(`gpt-6.1-sol`)을 자동 인식합니다.
- **단일 패스(One-Shot) 구조화**: 로컬에서 SponsorBlock 및 VTT 롤링 중복만 제거한 뒤, 100만 컨텍스트 무료 슬롯에 단 1회 요청하여 마크다운 노트를 생성합니다.

## 실행 방법
```bash
# 1. 의존성 설치
pip install -r requirements.txt

# 2. transcripts/ 폴더에 .vtt 또는 .srt 자막 파일 투입
# (파일명 권장 형식: 영상제목_[11자리ID].vtt)

# 3. 파이프라인 실행
python pipeline.py
```
생성된 구조화 문서는 `structured_notes/`에 저장되며, 처리 이력은 `subflow_state.db`에 자동 기록되어 재실행 시 중복 작업을 건너뜁니다.

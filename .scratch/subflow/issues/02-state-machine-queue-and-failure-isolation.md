# 02: 영속 상태 머신 큐 및 장애 격리 엔진

**What to build:** 자막 파일의 SHA-256 해시를 고유 키로 사용하는 로컬 SQLite 상태 머신을 구축하여, 중복 처리를 방지하고 예기치 못한 중단 시 중단된 지점부터 즉시 재개할 수 있는 멱등성 보장형 작업 큐 및 실패 격리 메커니즘.

**Blocked by:** 01: 자막 정규화 및 슬라이딩 윈도우 중복 제거기

**Status:** ready-for-agent

- [ ] SQLite 테이블 `jobs(file_hash, file_name, video_id, cleaned_text, status, retries, updated_at)` 및 `idx_jobs_status` 인덱스를 초기화한다.
- [ ] 입력 디렉터리의 자막 파일을 읽어 SHA-256 해시를 계산하고, 미등록된 파일만 정제 후 `PENDING` 상태로 큐에 등록한다.
- [ ] `status = 'PENDING'` 및 `retries < 5` 조건의 작업을 최대 100개씩 원자적으로 인출한다.
- [ ] 작업 실패 시 예외를 포착하여 `retries`를 1 증가시키며, 재시도 횟수가 5회에 도달하면 상태를 영구적으로 `FAILED`로 전이하여 큐 오염을 방지한다.
- [ ] 프로세스가 비정상 종료(SIGKILL/재부팅)되더라도 재실행 시 `PENDING` 상태의 작업부터 유실 없이 작업을 재개한다.

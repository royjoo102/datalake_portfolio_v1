# AdStream Data Lake 프로젝트 마일스톤

## 1. 프로젝트 목표

Athena에서 승인된 방식으로 내려받은 이벤트 데이터를 로컬 환경에서 재생하고, Kafka와 Spark Structured Streaming을 통해 필요한 필드만 선별하여 분석 가능한 데이터 레이크와 대시보드로 제공한다.

### 핵심 문제

- 수집 이벤트에 분석에 필요하지 않은 컬럼이 과도하게 포함되어 있다.
- 회사 정책상 원본 회사 S3에 직접 연결할 수 없다.
- 제한된 환경에서도 production과 유사한 이벤트 흐름을 재현할 필요가 있다.
- 정제 효과와 파이프라인 성능을 수치로 확인할 수 있어야 한다.

### 목표 아키텍처

```text
Athena Export (CSV/Parquet)
    → Python Replay Service
    → Kafka Raw Topic
    → Spark Structured Streaming
    → Silver/Rejected/Gold Data
    → DuckDB 또는 Athena
    → Streamlit Dashboard
```

### 목표 일정

- **로컬 MVP:** 2주
- **AWS 전환 및 포트폴리오 완성:** 추가 2주
- **전체 예상 기간:** 약 4주

---

## 2. 성공 KPI

| KPI | 초기 목표 |
|---|---:|
| 테스트 이벤트 | 1,000,000건 |
| 최대 Replay 처리량 | 1,000 events/sec |
| 정상 이벤트 처리율 | 99% 이상 |
| 필드 수 감소 | 예: 80개 → 15개 |
| 저장 용량 감소율 | 70% 이상 |
| 중복 처리 | 중복 이벤트 100% 제거 또는 격리 |
| 오류 이벤트 처리 | 100% Rejected Zone 또는 DLQ 이동 |
| End-to-end latency | 60초 미만 |
| 장애 복구 | 마지막 checkpoint 또는 offset부터 재처리 |

실제 측정 결과가 목표와 다르면 최종 보고서에 목표 대비 결과, 원인 및 후속 조치를 기록한다.

---

## 3. 프로젝트 마일스톤 요약

| 마일스톤 | 일정 | 주요 결과물 | 완료 기준 |
|---|---|---|---|
| M0. 프로젝트 정의 | 1~2일차 | Charter, 요구사항, KPI, 데이터 정책 범위 | 목표·범위·성공 기준 승인 |
| M1. 로컬 개발환경 | 3일차 | Docker Compose, Kafka, Spark, MinIO | 모든 서비스가 로컬에서 정상 실행 |
| M2. Replay Service | 4~5일차 | Python Replay CLI, Kafka Producer | 파일 이벤트를 지정 속도로 Kafka에 전송 |
| M3. Spark 정제 파이프라인 | 6~8일차 | Schema validation, allowlist, deduplication | 정상/오류 이벤트가 정확히 분리됨 |
| M4. 로컬 Data Lake | 9일차 | Silver, Gold, Rejected Parquet | 정제 데이터가 partition 단위로 저장됨 |
| M5. Dashboard | 10일차 | DuckDB query, Streamlit dashboard | 데이터 품질·성능·비즈니스 지표 확인 가능 |
| M6. CI 파이프라인 | 11~12일차 | GitHub Actions, unit/integration tests | PR마다 자동 검증 및 Docker build 성공 |
| M7. 로컬 MVP 검증 | 13~14일차 | 1M 부하 테스트, 복구 테스트, 결과 보고서 | 로컬 MVP Definition of Done 충족 |
| M8. AWS 전환 | 3주차 | S3, Glue, Athena, ECR 및 선택적 ECS | 정제 데이터를 AWS에서 조회 가능 |
| M9. 포트폴리오 완성 | 4주차 | README, 아키텍처, ADR, RAID, 데모 | 제3자가 프로젝트를 이해하고 재현 가능 |

---

## 4. 상세 마일스톤

## M0. 프로젝트 정의 및 데이터 계약

**기간:** 1~2일차  
**목표:** 구현 전에 문제, 범위, schema와 성공 기준을 확정한다.

### 주요 작업

- 사용자와 비즈니스 use case 정의
- 입력 이벤트 schema 분석
- 보존할 컬럼의 allowlist 정의
- 필수 필드와 nullable 필드 정의
- PII 또는 민감 정보 제거·마스킹 기준 정의
- 정상, 중복, 오류 이벤트의 처리 규칙 정의
- 기능·비기능 요구사항 작성
- 프로젝트 KPI 및 비용 제한 설정

### 산출물

- `program-charter.md`
- `requirements.md`
- `schema-contract.yaml`
- `filter-rules.yaml`
- 초기 RAID Log

### Exit Criteria

- 입력과 출력 schema가 문서화되어 있다.
- 제거 또는 유지할 필드의 근거가 기록되어 있다.
- 회사 데이터는 저장·공개 정책에 맞게 익명화되거나 synthetic data로 교체된다.
- MVP 포함 및 제외 범위가 명확하다.

---

## M1. 로컬 개발환경 구축

**기간:** 3일차  
**목표:** 전체 시스템을 로컬에서 반복 실행할 수 있는 기반을 만든다.

### 주요 작업

- Repository 구조 생성
- Docker Compose 설정
- Kafka KRaft broker 및 topic 생성
- Spark 실행 환경 구성
- MinIO bucket과 기본 폴더 구성
- 환경변수 기반 local/AWS 설정 분리
- 서비스 health check 추가

### 산출물

- `docker-compose.yml`
- `.env.example`
- Kafka raw/rejected topic 설정
- 로컬 실행 가이드

### Exit Criteria

- 단일 명령으로 Kafka, Spark, MinIO가 실행된다.
- Producer와 Consumer smoke test가 성공한다.
- 코드에 bucket, endpoint 또는 credential이 hard-coded되어 있지 않다.

---

## M2. Python Replay Service 구현

**기간:** 4~5일차  
**목표:** Athena export 파일을 production event처럼 Kafka에 재생한다.

### 주요 작업

- CSV/Parquet streaming reader 구현
- Kafka Producer 구현
- Replay 속도 설정 기능 추가
- `replay_run_id`, `replay_time`, `source_file` 추가
- 원본 이벤트 순서 및 original event time 유지
- pause, resume, retry 및 progress logging 구현
- replay 실패 이벤트 기록

### 주요 실행 예시

```bash
python -m replay \
  --source data/events.parquet \
  --topic raw-events \
  --rate 1000 \
  --run-id run-001
```

### Exit Criteria

- 최소 10,000건의 이벤트를 손실 없이 Kafka에 전송한다.
- 지정한 events/sec에 맞춰 replay 속도를 조절한다.
- 동일 파일을 동일한 조건으로 반복 실행할 수 있다.
- 전송 성공, 실패 및 처리량을 로그로 확인할 수 있다.

---

## M3. Spark 이벤트 정제 파이프라인 구현

**기간:** 6~8일차  
**목표:** Kafka 원본 이벤트에서 분석에 필요한 데이터만 선별한다.

### 주요 작업

- Spark Structured Streaming Kafka consumer 구현
- 명시적 schema 적용
- YAML 기반 allowlist field selection
- 필수 필드와 데이터 타입 검증
- `event_id` 기반 deduplication
- timestamp 정규화 및 event-time 처리
- 유효 이벤트와 rejected 이벤트 분리
- checkpoint 설정
- Kafka consumer lag 및 batch processing time 기록

### 오류 분류 예시

- `MISSING_REQUIRED_FIELD`
- `INVALID_DATA_TYPE`
- `UNSUPPORTED_SCHEMA_VERSION`
- `DUPLICATE_EVENT`
- `MALFORMED_JSON`

### Exit Criteria

- Silver 출력에는 allowlist 컬럼만 존재한다.
- 오류 이벤트에는 rejection reason이 기록된다.
- 중복 이벤트가 Silver에 중복 저장되지 않는다.
- Spark 재시작 후 checkpoint부터 처리를 재개한다.

---

## M4. 로컬 Data Lake 구축

**기간:** 9일차  
**목표:** 정제 데이터와 분석 데이터를 query 가능한 형식으로 저장한다.

### 저장 구조

```text
data-lake/
├── landing/athena-export/
├── silver/events/event_date=YYYY-MM-DD/event_type=.../
├── gold/event-volume/
├── gold/data-quality/
└── rejected/reason=.../
```

### 주요 작업

- MinIO에 Parquet 형식으로 저장
- `event_date`와 `event_type` 기반 partition 구성
- Silver 데이터 생성
- Dashboard용 Gold aggregation 생성
- Rejected Zone 구성
- small-file 발생 여부 확인

### Exit Criteria

- Silver, Gold, Rejected 데이터가 서로 분리된다.
- DuckDB에서 Parquet 데이터를 조회할 수 있다.
- 필터링 전후 컬럼 수와 파일 크기를 비교할 수 있다.

---

## M5. Dashboard 구축

**기간:** 10일차  
**목표:** 데이터 정제 효과와 파이프라인 상태를 한 화면에서 확인한다.

### Dashboard 항목

#### 데이터 품질

- 입력 이벤트 수
- 정상 처리 이벤트 수
- Rejected 이벤트 수와 사유
- 중복 제거 건수
- 필수 필드 누락률

#### 파이프라인 성능

- Events processed per second
- End-to-end latency
- Kafka consumer lag
- Spark micro-batch 처리 시간
- Replay 진행률

#### 정제 효과

- 입력/출력 컬럼 수
- 필터링 전후 데이터 용량
- 저장 용량 감소율

#### 비즈니스 지표

- 이벤트 유형별 발생량
- 채널·앱 버전별 이벤트 수
- 데이터에 포함된 경우 impression, click, conversion 및 CTR

### Exit Criteria

- DuckDB가 Gold Parquet을 조회한다.
- Streamlit에서 KPI와 추세를 확인할 수 있다.
- 새로운 replay run의 결과를 dashboard에서 구분할 수 있다.

---

## M6. CI 파이프라인 통합

**기간:** 11~12일차  
**목표:** 코드와 데이터 계약 변경을 자동 검증한다.

### Pull Request CI

- Ruff lint 및 formatting check
- Pytest unit tests
- Replay Service 테스트
- Spark transformation 테스트
- Schema contract 테스트
- Docker image build
- Dependency 및 secret scan

### End-to-End Integration Test

1. Synthetic event 1,000건 생성
2. Replay Service에서 Kafka로 전송
3. Spark가 이벤트 consume 및 정제
4. Silver와 Rejected 결과 생성
5. 다음 조건 자동 검증

```text
input_count = silver_count + rejected_count
silver_columns ⊆ allowed_columns
duplicate_event_count = 0
required_fields_null_count = 0
```

### Exit Criteria

- 모든 Pull Request에서 CI가 자동 실행된다.
- 코드 또는 schema 오류가 있으면 merge 전에 실패한다.
- Docker image가 재현 가능하게 생성된다.
- secret과 실제 회사 데이터가 repository에 포함되지 않는다.

---

## M7. 로컬 MVP 성능 및 복구 검증

**기간:** 13~14일차  
**목표:** 100만 건 기준으로 기능, 성능, 품질과 복구 능력을 검증한다.

### Test Scenarios

- 100,000건 baseline test
- 1,000,000건 sustained-load test
- 1,000 events/sec burst test
- Spark job 강제 종료 및 checkpoint recovery
- 잘못된 schema와 누락 필드 주입
- 동일 이벤트 반복 replay
- Kafka offset reset을 통한 processing replay
- rejected event 수정 후 재처리

### 산출물

- Performance Test Report
- Data Quality Report
- Before/After 데이터 크기 비교
- 장애 복구 결과
- 발견된 병목 및 개선 backlog

### Local MVP Definition of Done

- 100만 건이 Replay → Kafka → Spark → Data Lake로 처리된다.
- 필요한 컬럼만 Silver에 저장된다.
- 오류 이벤트는 사유와 함께 격리된다.
- Dashboard에서 처리량, latency 및 정제 효과를 확인할 수 있다.
- CI가 unit, schema 및 integration test를 통과한다.

---

## M8. AWS 전환 및 CD 구축

**기간:** 3주차  
**목표:** 로컬 MVP를 최소 변경으로 AWS 분석 환경에 연결한다.

### 권장 이전 순서

1. MinIO → Amazon S3
2. DuckDB → AWS Glue Catalog 및 Athena
3. Docker image → Amazon ECR
4. Replay Service → ECS 또는 EC2
5. 필요 시 Kafka → Amazon MSK
6. 필요 시 Spark → Amazon EMR

MSK와 EMR은 비용과 운영 복잡도가 높으므로, 첫 AWS milestone에서는 S3, Glue, Athena와 ECR을 우선한다.

### CD Trigger

| Trigger | 동작 |
|---|---|
| Pull Request | Test만 실행 |
| Main merge | Docker image 생성 |
| Version tag | ECR push 및 Dev 배포 |
| Production | Manual approval 후 배포 |

### Exit Criteria

- 로컬과 AWS가 동일한 application code를 사용한다.
- 환경변수만으로 storage와 endpoint를 전환할 수 있다.
- S3 정제 데이터를 Athena에서 조회할 수 있다.
- AWS Dev 배포 후 smoke test가 성공한다.
- Budget alarm과 lifecycle policy가 설정된다.

---

## M9. 포트폴리오 및 발표 자료 완성

**기간:** 4주차  
**목표:** 기술 구현뿐 아니라 TPM의 의사결정과 실행 관리 역량을 보여준다.

### 최종 산출물

- Executive Summary
- Program Charter
- Architecture Diagram
- Functional/Non-functional Requirements
- Schema Contract 및 Data Flow
- Roadmap 및 Milestone Status
- RAID Log
- Architecture Decision Records
- Test Strategy 및 Release Criteria
- Performance/Data Quality Report
- Cost Analysis
- Demo video 또는 screenshots
- Retrospective 및 Phase 2 Roadmap

### 권장 ADR

- ADR-001: Kafka를 이벤트 버퍼로 선택한 이유
- ADR-002: Blacklist 대신 Allowlist filtering을 선택한 이유
- ADR-003: Parquet과 partition strategy 선택
- ADR-004: 로컬 MinIO에서 AWS S3로 전환하는 방식
- ADR-005: Dashboard가 Spark에 직접 연결하지 않는 이유

### Exit Criteria

- README만 읽어도 문제, 설계, 결과와 trade-off를 이해할 수 있다.
- 제3자가 synthetic sample로 로컬 환경을 재현할 수 있다.
- KPI 목표와 실제 결과가 비교되어 있다.
- 본인의 TPM ownership과 기술 기여가 명확하게 구분된다.

---

## 5. 주요 의존성

| 의존성 | 영향을 받는 마일스톤 | 대응 |
|---|---|---|
| 승인된 Athena export 확보 | M0, M2 | Synthetic dataset을 병행 준비 |
| 입력 schema 확정 | M2, M3 | Schema versioning 적용 |
| Docker 실행 환경 | M1 이후 전체 | 초기 smoke test로 사전 확인 |
| Kafka 안정성 | M2~M7 | Local retention 및 topic 설정 문서화 |
| Spark checkpoint 경로 | M3~M7 | Replay run별 독립 checkpoint 사용 |
| AWS 계정과 IAM | M8 | AWS 단계 전 권한 목록 준비 |
| AWS 비용 | M8 | 리소스 자동 종료 및 budget alarm |

---

## 6. 주요 위험 및 완화 계획

| 위험 | 영향 | 완화 계획 |
|---|---|---|
| 회사 데이터가 외부 저장소에 노출 | 보안 및 정책 위반 | Synthetic/anonymized data만 공개 환경에서 사용 |
| 100만 건 테스트가 로컬 메모리 초과 | 처리 실패 | Batch 단위 Parquet read 및 backpressure 적용 |
| Kafka consumer lag 증가 | 분석 지연 | Partition 수와 Spark parallelism 조정 |
| Schema 변경으로 Spark job 실패 | 파이프라인 중단 | Schema version과 rejected routing 적용 |
| 중복 replay로 지표 왜곡 | 결과 부정확 | `event_id`와 watermark 기반 deduplication |
| Small files 증가 | 조회 성능 저하 | Batch size 조정 및 compaction backlog 등록 |
| AWS 전환이 지연됨 | 전체 일정 지연 | 로컬 MVP를 독립적인 포트폴리오 산출물로 완성 |
| 기술 범위 과다 | 일정 지연 | MSK, EMR, Iceberg는 Phase 2로 분리 가능 |

---

## 7. Scope Control

### MVP 포함

- Python Replay Service
- Docker 기반 Kafka
- Spark Structured Streaming
- Allowlist filtering
- Schema validation 및 deduplication
- Silver/Gold/Rejected Parquet
- DuckDB 및 Streamlit dashboard
- GitHub Actions CI
- 100만 건 성능 테스트

### Phase 2 후보

- Amazon MSK 및 EMR 완전 이전
- Apache Iceberg
- Trino 및 Superset
- Airflow orchestration
- Terraform 전체 자동화
- Schema Registry
- Prometheus/Grafana
- Multi-region disaster recovery

---

## 8. 이력서용 예상 결과 문장

> Designed and delivered a policy-compliant event replay and data lake pipeline that processed 1M events through Python, Kafka, and Spark Structured Streaming, applied configurable allowlist-based filtering and data quality controls, and published curated Parquet datasets and operational dashboards with automated CI/CD validation.

실제 완성 후에는 `1M`, 처리량, latency, 저장 용량 감소율을 측정 결과로 교체한다.

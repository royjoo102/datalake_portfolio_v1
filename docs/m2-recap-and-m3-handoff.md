# M2 회고 및 M3 인계

## 상태와 검증 근거

M2의 합의된 범위(CSV Replay)는 기능 검증을 완료했다. Kafka 저장 경로와 Docker 볼륨의 불일치는 별도 미해결 인프라 항목이다. 원본 보존 요구를 운영 환경에서도 충족하려면 컨테이너 재생성 전에 이 항목을 보완해야 한다.

| 항목 | 결과 |
| --- | --- |
| 입력 | `GCE_EVO_Data/GCE_EVO_202606.csv`만 사용 |
| 원본 규모 | 864,935행, 53개 컬럼, 630,081,197 bytes |
| CSV 구조 검사 | 전체 파일 순차 읽기 통과; 업무 데이터 품질 검증과는 별개 |
| 최초 Kafka 검증 | 4,000행 전송 후 재개하여 총 10,000행 대조 성공 |
| 속도 검증 | 재개 세션에서 설정 100건/초 대비 99.9건/초 |
| Unit test | 구현 시 9개 통과 |
| 이번 재확인 | 로컬 체크포인트 12,589행, `complete=false` |
| 이번 Kafka 재조회 | 동일 run의 12,589건을 원본과 대조하여 값·순서·ID 일치 확인 |
| 전체 파일 전송 | 미완료; 저장된 진행 위치 기준 852,346행 남음 |
| 1,000건/초·100만 건·실제 Broker 장애 복구 | 미검증, 후속 성능/복구 검증 범위 |

검증 run ID는 `m2-validation-20260908`이다. 10,000건은 최초 완료 기준의 검증 기록이며, 현재 전송 건수로 고정해서 해석하지 않는다. 위 결과는 2026-09-08 확인 시점의 기록이다.

## 대화에서 확정한 M2 결정

- 원본 모든 컬럼, 빈 값, 동일 내용의 행을 보존한다. 타입 변환, 필터링, 업무상 중복 제거는 M3에서 정의한다.
- `metadata`에는 run ID, replay 시각, 파일명/해시, 원본 행 번호, 행 식별 ID를 넣고 `payload`에는 원본 53개 컬럼을 문자열로 넣는다.
- `event_id`는 파일 해시와 원본 행 번호에서 생성한다. 실제 업무 이벤트의 고유 키라는 의미는 아니다.
- 원본 CSV 행 순서대로 전송한다. `event_timestamp` 기준 정렬은 현재 구현하거나 확정하지 않았다.
- `raw-events`와 `rejected-events`는 확인 당시 각각 3개 파티션이며 Replay는 기본 파티션 0만 사용한다. 내부 토픽 `__consumer_offsets`는 50개 파티션이다.
- Kafka offset은 파티션 내 기록 위치이며 CSV 행 번호와 다르다. 다른 실행이나 smoke-test 메시지가 함께 있을 수 있다.
- 체크포인트는 로컬 `checkpoints/replay/<run-id>.json`에 기록한다. Kafka 메시지의 `source_row`는 추적 정보이며 재개 시 체크포인트를 대신하지 않는다.
- 성공 응답 후 진행 위치를 갱신하고 100건마다 및 정상 중단/처리 오류 때 파일에 저장한다. 강제 종료나 응답 불확실성 뒤에는 일부 재전송이 가능하다.
- Raw 이벤트는 현재 Kafka에서 조회한다. Spark 소비 작업 및 MinIO로 이벤트를 저장하는 작업은 아직 없다.
- Parquet 입력은 후속 지원으로 남겼다.

## 코드 설명에서 다룬 구성

| 위치 | 역할 |
| --- | --- |
| `replay/__init__.py` | Python 패키지 구성 |
| `replay/__main__.py` | `python -m replay` 진입점 |
| `replay/cli.py` | 옵션, 파일 해시, CSV 읽기, 메시지 구성, 전송, 속도, 재개 관리 |
| `tests/test_replay.py` | 임시 CSV와 테스트용 Producer로 정상/실패 동작 검증 |
| `scripts/verify-replay.py` | 실제 Kafka 메시지를 원본 CSV의 첫 N행과 대조 |
| `__pycache__/` | Python 자동 생성 캐시; 이벤트나 체크포인트 저장소가 아님 |

Unit test는 코드의 판단을 통제된 조건에서 검사한다. 실제 Kafka의 연결/저장/재조회는 연동 검증으로 확인한다. 두 검증을 모두 수행했으며, Unit test만으로 실제 Broker 장애 복구까지 검증했다고 해석하지 않는다.

## 현재 조회 명령

프로젝트 루트에서 실행한다.

```bash
source datalake_env/bin/activate
python scripts/verify-replay.py --run-id m2-validation-20260908 --expected 12589

docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic raw-events \
  --partition 0 --from-beginning --max-messages 5
```

검증 명령의 건수는 이번 확인 시점의 값이다. 이후 같은 run을 계속 보내면 해당 run의 전체 전송 건수에 맞춰야 한다. Kafka 보존 정책으로 앞부분이 삭제되었다면 전체 대조는 실패할 수 있다. 검증 스크립트는 새 이벤트를 전송하거나 소비자 offset을 커밋하지 않는다.

## 발견 사항: Kafka 저장 경로 불일치

실행 중인 Broker에서 다음 파티션 파일을 확인했다.

```text
/tmp/kafka-logs/raw-events-0/
  00000000000000000000.log
  00000000000000000000.index
  00000000000000000000.timeindex
```

반면 `docker-compose.yml`은 `kafka-data:/var/lib/kafka/data`를 마운트하고 있으며 `KAFKA_LOG_DIRS`는 지정하지 않는다. `/tmp/kafka-logs`는 해당 볼륨으로 향하는 심볼릭 링크도 아니다. 따라서 앞선 대화의 “이벤트가 kafka-data 볼륨에 저장된다”는 설명은 실제 실행 상태와 달라 정정한다.

현재 파티션 데이터는 컨테이너 재생성 시 사라질 수 있다. `docker compose down` 후 `up`은 기존 컨테이너를 교체하므로, 저장 경로를 보완하기 전 일시 중지는 `docker compose stop`, 재시작은 `docker compose start`를 사용한다. 로컬 Replay 체크포인트가 남아 있더라도 Broker 데이터가 사라지면 누락을 자동 복원하지 못한다.

후속 작업:

1. Producer를 중단하고 현재 Kafka 로그 및 KRaft 메타데이터를 일관되게 백업한다.
2. 실제 Kafka 저장 경로와 볼륨 경로를 일치시키고 기존 데이터를 이전한다. 경로 설정만 변경하고 재생성하지 않는다.
3. 재생성 전후 토픽·offset·원본 대조로 보존 여부를 검증한다.

이번 회고에서는 발견 사항과 후속 절차를 기록했다. Broker 재시작, 경로 변경 및 데이터 이전은 수행하지 않았다.

## M3에서 정할 사항

아래는 논의한 설계 후보와 미결정 사항이며, 구현 완료나 최종 승인된 규칙을 의미하지 않는다.

| 항목 | 결정할 내용 |
| --- | --- |
| 이벤트 유형 | `event_type`과 `attributes_event_type`의 의미 및 분석 기준 |
| 정제 계약 | 필수/nullable 필드, 타입, 보존 컬럼, 오류 분류 |
| 중복 | 업무상 고유 키, 판정 기간, run 사이 중복 처리 |
| 이벤트 시각 | `event_timestamp` 단위와 유효성 확인 후 시간 타입으로 정규화 |
| 지연 도착 | 실제 분포를 확인하고 허용 지연 및 watermark 적용 여부 결정 |
| 원본 보존 | 집계에서 제외되는 지연/오류 이벤트도 원본으로 남길 경로와 보존 기간 결정 |
| 집계 보정 | 늦은 이벤트의 과거 집계 재계산, 결과 교체 및 재처리 중복 방지 |
| Spark 복구 | Replay 로컬 체크포인트와 별개의 Spark checkpoint 설계 |

정렬과 이벤트 시간 처리는 목적이 다르다. Kafka 저장 순서를 바꾸려면 전송 전 정렬이 필요하지만, 시간대별 집계는 이벤트 시각을 기준으로 처리할 수 있다. 최종 시간순 조회는 `ORDER BY`로 수행한다. 원본을 수동 편집하거나 M2 전송 순서를 변경하기로 결정한 것은 아니다.

Spark watermark는 이벤트를 정렬하지 않는다. 관측한 최대 이벤트 시각을 기준으로 상태 유지 범위를 관리하며, 허용 범위보다 오래된 데이터의 집계 반영은 보장되지 않는다. [Spark 3.5.8 공식 설명](https://spark.apache.org/docs/3.5.8/structured-streaming-programming-guide.html#semantic-guarantees-of-aggregation-with-watermarking)

과거 CSV를 Replay하는 시각과 원본 발생 시각의 차이를 수집 지연으로 계산하지 않는다. 시간순이 아닌 파일에 작은 watermark를 바로 적용하면 뒤늦게 읽은 과거 행이 집계에서 빠질 수 있으므로, 데이터 분포와 원본 보존 경로를 먼저 확인한다. 원본 전체 저장 + 실시간 잠정 집계 + 과거 집계 보정은 논의한 설계 후보다.

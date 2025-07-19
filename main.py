import json
import time
import requests
from datetime import datetime

"""
Kafka Manager
"""

from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import NoBrokersAvailable, TopicAlreadyExistsError


class KafkaManager:
    def __init__(self, bootstrap_servers: str = "localhost:9092"):
        self.bootstrap_servers = bootstrap_servers
        self.admin_client = None
        self.producer = None
        self.consumers = {}

    def create_admin_client(self):
        """관리자 클라이언트 생성"""
        try:
            self.admin_client = KafkaAdminClient(
                bootstrap_servers=self.bootstrap_servers, client_id="kafka_admin"
            )
            print("✅ Kafka 관리자 클라이언트가 생성되었습니다.")

            return True
        except NoBrokersAvailable:
            print("Kafka 브로커에 연결할 수 없습니다.")

            return False

    def create_topic(
        self,
        topic_name: str,
        num_partitions: int = 3,
        replication_factor: int = 1,
    ):
        """토픽 생성"""
        if not self.admin_client:
            if not self.create_admin_client():
                return False

        try:
            topic = NewTopic(
                name=topic_name,
                num_partitions=num_partitions,
                replication_factor=replication_factor,
            )

            self.admin_client.create_topics([topic])
            print(f"토픽 {topic_name}이 생성되었습니다.")

            return True

        except TopicAlreadyExistsError:
            print(f"토픽 '{topic_name}'이 이미 존재합니다.")

            return True

        except Exception as e:
            print(f"토픽 생성 실패: {e}")

            return False

    def list_topics(self):
        """토픽 목록 조회"""
        if not self.admin_client:
            if not self.create_admin_client():
                return []

        try:
            topics = self.admin_client.list_topics()
            print(f"사용 가능한 토픽: {topics}")

            return topics

        except Exception as e:
            print(f"토픽 목록 조회 실패: {e}")
            return []


"""
Kafka Producer
"""


class KafkaProducerManager:
    def __init__(self, bootstrap_servers: str = "localhost:9092"):
        self.bootstrap_servers = bootstrap_servers
        self.producer = None

    def create_producer(self):
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode(
                    "utf-8"
                ),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",  # 모든 복제본이 메시지를 받았는지 확인
                retries=3,
                batch_size=16384,  # 배치 크기
                linger_ms=10,  # 배치 대기 시간
                compression_type="gzip",  # 압축 타입
            )
            print("Kafka 프로듀서가 생성되었습니다.")
            return True

        except Exception as e:
            print(f"프로듀서 생성 실패: {e}")
            return False

    def send_message(self, topic: str, message: dict, key: str = None):
        """메시지 전송"""
        if not self.producer:
            if not self.create_producer():
                return False

        try:
            future = self.producer.send(
                topic=topic,
                key=key,
                value=message,
            )
            record_metadata = future.get(timeout=10)
            print(
                f"메시지 전송 완료: {message} -> 파티션 {record_metadata.partition}, 오프셋: {record_metadata.offset}"
            )

        except Exception as e:
            print(f"메시지 전송 실패: {e}")
            return False

    def close(self):
        """프로듀서 종료"""
        if self.producer:
            self.producer.close()
            print("Kafka 프로듀서가 종료되었습니다.")


"""
Kafka Consumer
"""


class KafkaConsumerManager:
    def __init__(self, bootstrap_servers: str = "localhost:9092"):
        self.bootstrap_servers = bootstrap_servers
        self.consumer = None
        self.consumers = {}

    def create_consumer(
        self,
        group_id: str,
        topic: str,
        auto_offset_reset: str = "earliest",
    ):
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=self.bootstrap_servers,
                group_id=group_id,
                auto_offset_reset=auto_offset_reset,
                enable_auto_commit=True,
                auto_commit_interval_ms=1000,
                consumer_timeout_ms=5000,  # 5초 타임아웃
                max_poll_records=500,  # 한 번에 가져올 최대 레코드 수
                session_timeout_ms=30000,  # 세션 타임아웃
            )

            self.consumers[group_id] = consumer
            print(f"컨슈머 그룹 '{group_id}'이 생성되었습니다.")
            return consumer

        except Exception as e:
            print(f"컨슈머 생성 실패: {e}")

            return False

    def consume_message(
        self,
        group_id: str,
        topic: str,
        max_messages: int = 10,
    ):
        consumer = self.consumers.get(group_id)
        if not consumer:
            consumer = self.create_consumer(group_id, topic)
            if not consumer:
                return

        print(f"{topic} 에서 메시지를 소비하겠습니다.")
        print(f"컨슈머 설정: auto_offset_reset=earliest, timeout=5000ms")

        message_count = 0
        try:
            print("메시지 대기 중...")
            for message in consumer:
                print(
                    f"📥 메시지 수신: 파티션={message.partition}, 오프셋={message.offset}"
                )
                print(f"   키: {message.key}")
                # 메시지 값 디코딩
                try:
                    if message.value:
                        value = json.loads(message.value.decode("utf-8"))
                    else:
                        value = message.value
                except:
                    value = message.value

                print(f"   값: {value}")
                print(
                    f"   타임스탬프: {datetime.fromtimestamp(message.timestamp / 1000)}"
                )
                print("-" * 50)

                message_count += 1

                if message_count >= max_messages:
                    break

                # Send to n8n
                try:
                    response = requests.post(
                        "https://matagi.app.n8n.cloud/webhook-test/72affee4-cd2c-4592-937a-b0e54b4dfa24",
                        json=value,
                    )
                    print(f"n8n 전송 완료: {response.status_code}")

                except Exception as e:
                    print(f"n8n 전송 실패: {e}")

        except Exception as e:
            print(f"{topic} 메시지 소비 중 실패 count: {message_count}")

        finally:
            consumer.close()


######
"""
ClickHouse Manager
"""
import clickhouse_connect
from clickhouse_connect.driver.client import Client


class ClickHouseManager:
    def __init__(
        self,
        host: str = "localhost",
        port: int = 8123,
        username: str = "default",
        password: str = "",
        database: str = "default",
    ):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database
        self.client = None

    def connect_to_clickhouse(self):
        try:
            self.client = clickhouse_connect.get_client(
                host=self.host,
                port=self.port,
                username=self.username,
                password=self.password,
                database=self.database,
            )
            print("클릭하우스 데이터베이스에 성공적으로 연결되었습니다!")

            return True
        except Exception as e:
            print(f"클릭하우스 연결 실패: {e}")

            return False

    def insert_data(self, data):
        try:
            pass
        except Exception as e:
            print(f"데이터 삽입 중 실패")


#### MAIN
if __name__ == "__main__":
    kafka_manager = KafkaManager()
    kafka_producer = KafkaProducerManager()
    kafka_consumer = KafkaConsumerManager()
    clickhouse_manager = ClickHouseManager()

    # Kafka 토픽 생성
    NEW_TOPIC = "send_msg"
    kafka_manager.create_topic(NEW_TOPIC)

    # Kafka 메시지 전송
    kafka_producer.send_message(
        NEW_TOPIC,
        {
            "title": "자동화 테스트",
            "message": "n8n 자동화해서 메일 보내기 가능 테스트!",
            "target_email": "dev_matagi@kakao.com",
            "timestamp": datetime.now().isoformat(),
        },
    )

    time.sleep(2)

    # 메시지 소비 (새로운 그룹으로)
    kafka_consumer.consume_message("test_group_new", NEW_TOPIC)
    
    # clickhouse 저장
    get_msg_resp = kafka_consumer.consume_message("test_group", "get_msg")
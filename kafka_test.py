#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Kafka 학습용 종합 예제
프로듀서, 컨슈머, 토픽 관리, 스트리밍 처리 등을 포함
"""

import json
import time
import random
import threading
from datetime import datetime
from typing import Dict, List, Any
from dataclasses import dataclass, asdict

from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, NoBrokersAvailable

# from kafka.partitioner import RoundRobinPartitioner, HashedPartitioner


@dataclass
class OrderEvent:
    """주문 이벤트 데이터 클래스"""

    order_id: str
    customer_id: str
    product_name: str
    quantity: int
    price: float
    timestamp: str
    status: str = "pending"


@dataclass
class UserActivity:
    """사용자 활동 데이터 클래스"""

    user_id: str
    action: str
    page: str
    session_id: str
    timestamp: str
    ip_address: str


class KafkaManager:
    """Kafka 관리 클래스"""

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
            print("❌ Kafka 브로커에 연결할 수 없습니다.")
            return False

    def create_topic(
        self, topic_name: str, num_partitions: int = 3, replication_factor: int = 1
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
            print(f"✅ 토픽 '{topic_name}'이 생성되었습니다.")
            return True

        except TopicAlreadyExistsError:
            print(f"ℹ️ 토픽 '{topic_name}'이 이미 존재합니다.")
            return True
        except Exception as e:
            print(f"❌ 토픽 생성 실패: {e}")
            return False

    def list_topics(self) -> List[str]:
        """토픽 목록 조회"""
        if not self.admin_client:
            if not self.create_admin_client():
                return []

        try:
            topics = self.admin_client.list_topics()
            print(f"📋 사용 가능한 토픽: {topics}")
            return topics
        except Exception as e:
            print(f"❌ 토픽 목록 조회 실패: {e}")
            return []

    def delete_topic(self, topic_name: str):
        """토픽 삭제"""
        if not self.admin_client:
            if not self.create_admin_client():
                return False

        try:
            self.admin_client.delete_topics([topic_name])
            print(f"✅ 토픽 '{topic_name}'이 삭제되었습니다.")
            return True
        except Exception as e:
            print(f"❌ 토픽 삭제 실패: {e}")
            return False


class KafkaProducerExample:
    """Kafka 프로듀서 예제"""

    def __init__(self, bootstrap_servers: str = "localhost:9092"):
        self.bootstrap_servers = bootstrap_servers
        self.producer = None

    def create_producer(self):
        """프로듀서 생성"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode(
                    "utf-8"
                ),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                # partitioner=HashedPartitioner(),
                acks="all",  # 모든 복제본이 메시지를 받았는지 확인
                retries=3,  # 재시도 횟수
                batch_size=16384,  # 배치 크기
                linger_ms=10,  # 배치 대기 시간
                compression_type="gzip",  # 압축 타입
            )
            print("✅ Kafka 프로듀서가 생성되었습니다.")
            return True
        except Exception as e:
            print(f"❌ 프로듀서 생성 실패: {e}")
            return False

    def send_order_event(self, topic: str, order: OrderEvent):
        """주문 이벤트 전송"""
        if not self.producer:
            if not self.create_producer():
                return False

        try:
            # 키로 order_id 사용 (같은 주문은 같은 파티션으로)
            future = self.producer.send(
                topic=topic, key=order.order_id, value=asdict(order)
            )

            # 비동기 전송 결과 확인
            record_metadata = future.get(timeout=10)
            print(
                f"📤 주문 이벤트 전송 완료: {order.order_id} -> 파티션 {record_metadata.partition}, 오프셋 {record_metadata.offset}"
            )
            return True

        except Exception as e:
            print(f"❌ 메시지 전송 실패: {e}")
            return False

    def send_user_activity(self, topic: str, activity: UserActivity):
        """사용자 활동 전송"""
        if not self.producer:
            if not self.create_producer():
                return False

        try:
            future = self.producer.send(
                topic=topic, key=activity.user_id, value=asdict(activity)
            )

            record_metadata = future.get(timeout=10)
            print(
                f"📤 사용자 활동 전송 완료: {activity.user_id} -> 파티션 {record_metadata.partition}, 오프셋 {record_metadata.offset}"
            )
            return True

        except Exception as e:
            print(f"❌ 메시지 전송 실패: {e}")
            return False

    def send_batch_messages(self, topic: str, messages: List[Dict[str, Any]]):
        """배치 메시지 전송"""
        if not self.producer:
            if not self.create_producer():
                return False

        try:
            for i, message in enumerate(messages):
                key = message.get("key", f"batch_{i}")
                self.producer.send(topic, key=key, value=message)

            # 모든 메시지가 전송될 때까지 대기
            self.producer.flush()
            print(f"✅ {len(messages)}개의 배치 메시지가 전송되었습니다.")
            return True

        except Exception as e:
            print(f"❌ 배치 메시지 전송 실패: {e}")
            return False

    def close(self):
        """프로듀서 종료"""
        if self.producer:
            self.producer.close()
            print("👋 프로듀서가 종료되었습니다.")


class KafkaConsumerExample:
    """Kafka 컨슈머 예제"""

    def __init__(self, bootstrap_servers: str = "localhost:9092"):
        self.bootstrap_servers = bootstrap_servers
        self.consumers = {}

    def create_consumer(
        self, group_id: str, topic: str, auto_offset_reset: str = "earliest"
    ):
        """컨슈머 생성"""
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=self.bootstrap_servers,
                group_id=group_id,
                auto_offset_reset=auto_offset_reset,
                enable_auto_commit=True,
                auto_commit_interval_ms=1000,
                value_deserializer=lambda x: json.loads(x.decode("utf-8")),
                key_deserializer=lambda x: x.decode("utf-8") if x else None,
                consumer_timeout_ms=1000,  # 1초 타임아웃
                max_poll_records=500,  # 한 번에 가져올 최대 레코드 수
                session_timeout_ms=30000,  # 세션 타임아웃
                heartbeat_interval_ms=3000,  # 하트비트 간격
            )

            self.consumers[group_id] = consumer
            print(f"✅ 컨슈머 그룹 '{group_id}'이 생성되었습니다.")
            return consumer

        except Exception as e:
            print(f"❌ 컨슈머 생성 실패: {e}")
            return None

    def consume_messages(self, group_id: str, topic: str, max_messages: int = 10):
        """메시지 소비"""
        consumer = self.consumers.get(group_id)
        if not consumer:
            consumer = self.create_consumer(group_id, topic)
            if not consumer:
                return

        print(f"🔄 '{topic}' 토픽에서 메시지를 소비합니다...")

        message_count = 0
        try:
            for message in consumer:
                print(
                    f"📥 메시지 수신: 파티션={message.partition}, 오프셋={message.offset}"
                )
                print(f"   키: {message.key}")
                print(f"   값: {message.value}")
                print(
                    f"   타임스탬프: {datetime.fromtimestamp(message.timestamp / 1000)}"
                )
                print("-" * 50)

                message_count += 1
                if message_count >= max_messages:
                    break

        except Exception as e:
            print(f"❌ 메시지 소비 중 오류: {e}")
        finally:
            consumer.close()
            print(f"👋 컨슈머 그룹 '{group_id}'이 종료되었습니다.")

    def consume_continuously(self, group_id: str, topic: str, callback=None):
        """지속적인 메시지 소비 (백그라운드 스레드)"""

        def consume_worker():
            consumer = self.consumers.get(group_id)
            if not consumer:
                consumer = self.create_consumer(group_id, topic)
                if not consumer:
                    return

            print(f"🔄 '{topic}' 토픽에서 지속적으로 메시지를 소비합니다...")

            try:
                for message in consumer:
                    if callback:
                        callback(message)
                    else:
                        print(f"📥 메시지: {message.value}")

            except Exception as e:
                print(f"❌ 지속적 소비 중 오류: {e}")
            finally:
                consumer.close()

        # 백그라운드 스레드로 실행
        thread = threading.Thread(target=consume_worker, daemon=True)
        thread.start()
        return thread

    def close_all(self):
        """모든 컨슈머 종료"""
        for group_id, consumer in self.consumers.items():
            consumer.close()
            print(f"👋 컨슈머 그룹 '{group_id}'이 종료되었습니다.")


def generate_sample_data():
    """샘플 데이터 생성"""

    # 주문 이벤트 샘플 데이터
    order_events = []
    products = ["노트북", "스마트폰", "태블릿", "헤드폰", "키보드", "마우스"]
    statuses = ["pending", "confirmed", "shipped", "delivered", "cancelled"]

    for i in range(10):
        order = OrderEvent(
            order_id=f"ORDER_{i+1:04d}",
            customer_id=f"CUST_{random.randint(1000, 9999)}",
            product_name=random.choice(products),
            quantity=random.randint(1, 5),
            price=random.uniform(10000, 500000),
            timestamp=datetime.now().isoformat(),
            status=random.choice(statuses),
        )
        order_events.append(order)

    # 사용자 활동 샘플 데이터
    user_activities = []
    actions = ["view", "click", "purchase", "login", "logout", "search"]
    pages = ["home", "products", "cart", "checkout", "profile", "search"]

    for i in range(15):
        activity = UserActivity(
            user_id=f"USER_{random.randint(100, 999)}",
            action=random.choice(actions),
            page=random.choice(pages),
            session_id=f"SESS_{random.randint(10000, 99999)}",
            timestamp=datetime.now().isoformat(),
            ip_address=f"192.168.1.{random.randint(1, 255)}",
        )
        user_activities.append(activity)

    return order_events, user_activities


def message_processor(message):
    """메시지 처리 콜백 함수"""
    try:
        data = message.value
        if "order_id" in data:
            print(
                f"🛒 주문 처리: {data['order_id']} - {data['product_name']} x{data['quantity']}"
            )
        elif "user_id" in data:
            print(
                f"👤 사용자 활동: {data['user_id']} - {data['action']} on {data['page']}"
            )
    except Exception as e:
        print(f"❌ 메시지 처리 오류: {e}")


def main():
    """메인 함수 - Kafka 예제 실행"""
    print("🚀 Kafka 학습용 예제를 시작합니다...")
    print("=" * 60)

    # Kafka 관리자 생성
    kafka_manager = KafkaManager()

    # 토픽 생성
    topics = ["orders", "user_activities", "analytics"]
    for topic in topics:
        kafka_manager.create_topic(topic, num_partitions=3, replication_factor=1)

    # 토픽 목록 확인
    kafka_manager.list_topics()

    # 프로듀서 생성 및 메시지 전송
    producer = KafkaProducerExample()

    # 샘플 데이터 생성
    order_events, user_activities = generate_sample_data()

    print("\n📤 주문 이벤트 전송 중...")
    for order in order_events:
        producer.send_order_event("orders", order)
        time.sleep(0.1)  # 메시지 간 간격

    print("\n📤 사용자 활동 전송 중...")
    for activity in user_activities:
        producer.send_user_activity("user_activities", activity)
        time.sleep(0.1)

    # 컨슈머 생성 및 메시지 소비
    consumer = KafkaConsumerExample()

    print("\n📥 주문 이벤트 소비 중...")
    consumer.consume_messages("order_consumer_group", "orders", max_messages=5)

    print("\n📥 사용자 활동 소비 중...")
    consumer.consume_messages(
        "activity_consumer_group", "user_activities", max_messages=5
    )

    # 지속적인 메시지 소비 예제 (백그라운드)
    print("\n🔄 지속적인 메시지 소비 시작...")
    analytics_thread = consumer.consume_continuously(
        "analytics_consumer_group", "orders", callback=message_processor
    )

    # 추가 메시지 전송 (지속적 소비 테스트)
    print("\n📤 추가 메시지 전송...")
    for i in range(3):
        new_order = OrderEvent(
            order_id=f"REALTIME_ORDER_{i+1}",
            customer_id=f"CUST_{random.randint(1000, 9999)}",
            product_name="실시간 상품",
            quantity=1,
            price=100000,
            timestamp=datetime.now().isoformat(),
            status="pending",
        )
        producer.send_order_event("orders", new_order)
        time.sleep(1)

    # 잠시 대기 후 종료
    time.sleep(3)

    # 리소스 정리
    producer.close()
    consumer.close_all()

    print("\n✅ Kafka 예제가 완료되었습니다!")


if __name__ == "__main__":
    main()

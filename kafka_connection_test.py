#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Kafka 연결 테스트
"""

from kafka import KafkaConsumer, KafkaProducer, KafkaAdminClient
from kafka.errors import NoBrokersAvailable, KafkaError
import json


def test_kafka_connection():
    """Kafka 연결 테스트"""
    print("🔍 Kafka 연결 테스트 시작")
    print("=" * 40)

    bootstrap_servers = "localhost:9092"

    # 1. AdminClient 연결 테스트
    print("1️⃣ AdminClient 연결 테스트...")
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
        topics = admin_client.list_topics()
        print(f"✅ AdminClient 연결 성공! 토픽 수: {len(topics)}")
        for topic in topics:
            if not topic.startswith("__"):
                print(f"   - {topic}")
        admin_client.close()
    except Exception as e:
        print(f"❌ AdminClient 연결 실패: {e}")

    # 2. Producer 연결 테스트
    print("\n2️⃣ Producer 연결 테스트...")
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        print("✅ Producer 연결 성공!")
        producer.close()
    except Exception as e:
        print(f"❌ Producer 연결 실패: {e}")

    # 3. Consumer 연결 테스트
    print("\n3️⃣ Consumer 연결 테스트...")
    try:
        consumer = KafkaConsumer(
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset="earliest",
            consumer_timeout_ms=1000,
        )
        print("✅ Consumer 연결 성공!")
        consumer.close()
    except Exception as e:
        print(f"❌ Consumer 연결 실패: {e}")

    # 4. 간단한 메시지 전송/수신 테스트
    print("\n4️⃣ 메시지 전송/수신 테스트...")
    try:
        # Producer 생성
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )

        # 테스트 메시지 전송
        test_message = {"test": "connection", "timestamp": "2024-01-01"}
        future = producer.send("orders", test_message)
        record_metadata = future.get(timeout=10)
        print(
            f"✅ 메시지 전송 성공: 파티션 {record_metadata.partition}, 오프셋 {record_metadata.offset}"
        )

        producer.close()

        # Consumer로 메시지 수신
        consumer = KafkaConsumer(
            "orders",
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset="latest",
            consumer_timeout_ms=2000,
            value_deserializer=lambda x: json.loads(x.decode("utf-8")) if x else None,
        )

        message_count = 0
        for message in consumer:
            if message_count >= 1:
                break
            print(f"✅ 메시지 수신 성공: {message.value}")
            message_count += 1

        consumer.close()

    except Exception as e:
        print(f"❌ 메시지 전송/수신 실패: {e}")


if __name__ == "__main__":
    test_kafka_connection()

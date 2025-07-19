#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Kafka 대시보드 - 메시지 현황 모니터링
"""

import json
import time
from datetime import datetime
from typing import Dict, List, Any
from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient
from kafka.errors import NoBrokersAvailable


class KafkaDashboard:
    """Kafka 대시보드 클래스"""

    def __init__(self, bootstrap_servers: str = "localhost:9092"):
        self.bootstrap_servers = bootstrap_servers
        self.admin_client = None
        self.producer = None
        self.consumers = {}

    def connect_admin(self):
        """관리자 클라이언트 연결"""
        try:
            self.admin_client = KafkaAdminClient(
                bootstrap_servers=self.bootstrap_servers
            )
            return True
        except NoBrokersAvailable:
            print("❌ Kafka 브로커에 연결할 수 없습니다.")
            return False

    def get_topics_info(self) -> List[Dict[str, Any]]:
        """토픽 정보 조회"""
        if not self.admin_client:
            if not self.connect_admin():
                return []

        try:
            topics = self.admin_client.list_topics()
            topics_info = []

            for topic in topics:
                if not topic.startswith("__"):  # 시스템 토픽 제외
                    topic_info = {
                        "name": topic,
                        "partitions": 0,
                        "messages": 0,
                        "size": 0,
                    }

                    # 토픽 상세 정보 조회
                    try:
                        from kafka import TopicPartition

                        consumer = KafkaConsumer(
                            bootstrap_servers=self.bootstrap_servers,
                            auto_offset_reset="earliest",
                        )

                        # 파티션 정보 조회
                        partitions = consumer.partitions_for_topic(topic)
                        if partitions:
                            topic_info["partitions"] = len(partitions)

                            # 각 파티션의 메시지 수 조회
                            total_messages = 0
                            for partition_id in partitions:
                                tp = TopicPartition(topic, partition_id)
                                consumer.assign([tp])
                                consumer.seek_to_beginning(tp)
                                beginning = consumer.position([tp])[0]
                                consumer.seek_to_end(tp)
                                end = consumer.position([tp])[0]
                                total_messages += end - beginning

                            topic_info["messages"] = total_messages

                        consumer.close()

                    except Exception as e:
                        print(f"토픽 {topic} 정보 조회 실패: {e}")

                    topics_info.append(topic_info)

            return topics_info

        except Exception as e:
            print(f"토픽 정보 조회 실패: {e}")
            return []

    def get_consumer_groups(self) -> List[Dict[str, Any]]:
        """컨슈머 그룹 정보 조회"""
        try:
            from kafka.admin import ConfigResource, ConfigResourceType
            from kafka.consumer.group import KafkaConsumer

            # 컨슈머 그룹 목록 조회
            consumer = KafkaConsumer(
                bootstrap_servers=self.bootstrap_servers, group_id="dashboard_consumer"
            )

            # 컨슈머 그룹 정보 조회
            groups = []
            try:
                # 간단한 방법으로 컨슈머 그룹 정보 조회
                for topic in self.admin_client.list_topics():
                    if not topic.startswith("__"):
                        groups.append(
                            {
                                "topic": topic,
                                "group_id": "unknown",
                                "members": 0,
                                "lag": 0,
                            }
                        )
            except:
                pass

            consumer.close()
            return groups

        except Exception as e:
            print(f"컨슈머 그룹 정보 조회 실패: {e}")
            return []

    def get_recent_messages(self, topic: str, limit: int = 10) -> List[Dict[str, Any]]:
        """최근 메시지 조회"""
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=self.bootstrap_servers,
                auto_offset_reset="latest",
                consumer_timeout_ms=1000,
                value_deserializer=lambda x: x.decode("utf-8") if x else None,
                key_deserializer=lambda x: x.decode("utf-8") if x else None,
            )

            messages = []
            message_count = 0

            # 최신 메시지부터 조회
            for message in consumer:
                if message_count >= limit:
                    break

                try:
                    # JSON 파싱 시도
                    value = (
                        json.loads(message.value) if message.value else message.value
                    )
                except:
                    value = message.value

                msg_info = {
                    "partition": message.partition,
                    "offset": message.offset,
                    "key": message.key,
                    "value": value,
                    "timestamp": datetime.fromtimestamp(
                        message.timestamp / 1000
                    ).isoformat(),
                    "headers": dict(message.headers) if message.headers else {},
                }

                messages.append(msg_info)
                message_count += 1

            consumer.close()
            return messages

        except Exception as e:
            print(f"메시지 조회 실패: {e}")
            return []

    def get_broker_info(self) -> Dict[str, Any]:
        """브로커 정보 조회"""
        try:
            broker_info = {
                "bootstrap_servers": self.bootstrap_servers,
                "connected": False,
                "topics_count": 0,
                "total_messages": 0,
            }

            # 연결 테스트
            if not self.admin_client:
                if not self.connect_admin():
                    return broker_info

            # 토픽 목록 조회로 연결 확인
            topics = self.admin_client.list_topics()
            broker_info["connected"] = True
            broker_info["topics_count"] = len(
                [t for t in topics if not t.startswith("__")]
            )

            return broker_info

        except Exception as e:
            print(f"브로커 정보 조회 실패: {e}")
            return {"connected": False}

    def display_dashboard(self):
        """대시보드 표시"""
        print("🎯 Kafka 대시보드")
        print("=" * 60)

        # 브로커 정보
        broker_info = self.get_broker_info()
        print(f"📡 브로커: {broker_info['bootstrap_servers']}")
        print(
            f"🔗 연결 상태: {'✅ 연결됨' if broker_info['connected'] else '❌ 연결 안됨'}"
        )

        if not broker_info["connected"]:
            print("💡 Kafka 서버가 실행 중인지 확인하세요.")
            return

        # 토픽 정보
        print(f"\n📋 토픽 정보")
        print("-" * 40)
        topics_info = self.get_topics_info()

        if topics_info:
            for topic in topics_info:
                print(f"📊 {topic['name']}")
                print(f"   파티션: {topic['partitions']}")
                print(f"   메시지 수: {topic['messages']:,}")
                print(f"   크기: {topic['size']} bytes")
                print()
        else:
            print("📭 토픽이 없습니다.")

        # 컨슈머 그룹 정보
        print(f"👥 컨슈머 그룹")
        print("-" * 40)
        consumer_groups = self.get_consumer_groups()

        if consumer_groups:
            for group in consumer_groups:
                print(f"🔸 {group['topic']}")
                print(f"   그룹 ID: {group['group_id']}")
                print(f"   멤버 수: {group['members']}")
                print(f"   지연: {group['lag']}")
                print()
        else:
            print("📭 컨슈머 그룹이 없습니다.")

        # 최근 메시지 샘플
        if topics_info:
            print(f"📨 최근 메시지 샘플")
            print("-" * 40)

            for topic in topics_info[:2]:  # 처음 2개 토픽만
                print(f"📤 {topic['name']} 토픽의 최근 메시지:")
                messages = self.get_recent_messages(topic["name"], limit=3)

                if messages:
                    for i, msg in enumerate(messages, 1):
                        print(
                            f"   {i}. 파티션 {msg['partition']}, 오프셋 {msg['offset']}"
                        )
                        print(f"      키: {msg['key']}")
                        print(f"      값: {str(msg['value'])[:100]}...")
                        print(f"      시간: {msg['timestamp']}")
                        print()
                else:
                    print("   📭 메시지가 없습니다.")
                print()


def real_time_monitor():
    """실시간 모니터링"""
    dashboard = KafkaDashboard()

    print("🔄 실시간 Kafka 모니터링 시작...")
    print("Ctrl+C로 종료")
    print("=" * 60)

    try:
        while True:
            dashboard.display_dashboard()
            print("⏳ 2초 후 업데이트...")
            time.sleep(2)
            print("\n" + "=" * 60 + "\n")

    except KeyboardInterrupt:
        print("\n👋 모니터링을 종료합니다.")


def main():
    """메인 함수"""
    dashboard = KafkaDashboard()

    # 한 번 실행
    dashboard.display_dashboard()
    # real_time_monitor()

    print("\n💡 추가 옵션:")
    print("1. 실시간 모니터링: real_time_monitor() 호출")
    print("2. 특정 토픽 메시지 조회: get_recent_messages('topic_name')")
    print("3. 토픽 정보 조회: get_topics_info()")


if __name__ == "__main__":
    main()

#!/bin/bash

echo "🚀 Kafka 학습 환경 실행 스크립트"
echo "=================================="

# Docker가 설치되어 있는지 확인
if ! command -v docker &> /dev/null; then
    echo "❌ Docker가 설치되어 있지 않습니다."
    echo "Docker를 먼저 설치해주세요: https://docs.docker.com/get-docker/"
    exit 1
fi

# Docker Compose가 설치되어 있는지 확인
if ! docker compose version &> /dev/null; then
    echo "❌ Docker Compose가 설치되어 있지 않습니다."
    echo "Docker Compose를 먼저 설치해주세요."
    exit 1
fi

echo "✅ Docker 및 Docker Compose 확인 완료"

# Kafka 실행
echo "📦 Kafka 환경을 시작합니다..."
docker compose -f docker-compose-kafka.yml up -d

# 컨테이너 상태 확인
echo "⏳ Kafka 서비스가 시작되는 동안 잠시 기다립니다..."
sleep 10

# 컨테이너 상태 확인
if docker ps | grep -q kafka && docker ps | grep -q zookeeper; then
    echo "✅ Kafka 환경이 성공적으로 실행되었습니다!"
    echo ""
    echo "📊 Kafka 접속 정보:"
    echo "  - Kafka 브로커: localhost:9092"
    echo "  - Zookeeper: localhost:2181"
    echo "  - Kafka UI: http://localhost:8080"
    echo ""
    echo "🔧 유용한 명령어:"
    echo "  - 서비스 중지: docker compose -f docker-compose-kafka.yml down"
    echo "  - 로그 확인: docker compose -f docker-compose-kafka.yml logs kafka"
    echo "  - 컨테이너 재시작: docker compose -f docker-compose-kafka.yml restart"
    echo ""
    echo "🐍 Python 스크립트 실행:"
    echo "  python kafka_test.py"
    echo ""
    echo "🌐 Kafka UI 접속:"
    echo "  브라우저에서 http://localhost:8080 으로 접속하여 Kafka 관리 UI를 사용할 수 있습니다."
else
    echo "❌ Kafka 환경 실행에 실패했습니다."
    echo "로그를 확인해보세요: docker compose -f docker-compose-kafka.yml logs"
fi 
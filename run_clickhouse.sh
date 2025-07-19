#!/bin/bash

echo "🚀 클릭하우스 데이터베이스 실행 스크립트"
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

# 클릭하우스 실행
echo "📦 클릭하우스 컨테이너를 시작합니다..."
docker compose up -d

# 컨테이너 상태 확인
echo "⏳ 클릭하우스 서버가 시작되는 동안 잠시 기다립니다..."
sleep 10

# 컨테이너 상태 확인
if docker ps | grep -q clickhouse-server; then
    echo "✅ 클릭하우스 서버가 성공적으로 실행되었습니다!"
    echo ""
    echo "📊 클릭하우스 접속 정보:"
    echo "  - HTTP 인터페이스: http://localhost:8123"
    echo "  - Native 포트: localhost:9000"
    echo "  - 기본 사용자: default"
    echo "  - 기본 비밀번호: (없음)"
    echo ""
    echo "🔧 유용한 명령어:"
    echo "  - 서버 중지: docker compose down"
    echo "  - 로그 확인: docker compose logs clickhouse"
    echo "  - 컨테이너 재시작: docker compose restart"
    echo ""
    echo "🐍 Python 스크립트 실행:"
    echo "  python click_house.py"
else
    echo "❌ 클릭하우스 서버 실행에 실패했습니다."
    echo "로그를 확인해보세요: docker compose logs clickhouse"
fi 
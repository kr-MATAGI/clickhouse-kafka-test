#!/bin/bash

echo "🚀 n8n 워크플로우 자동화 실행 스크립트"
echo "====================================="

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

# n8n 실행
echo "📦 n8n 컨테이너를 시작합니다..."
docker compose -f docker-compose-n8n.yml up -d

# 컨테이너 상태 확인
echo "⏳ n8n 서비스가 시작되는 동안 잠시 기다립니다..."
sleep 15

# 컨테이너 상태 확인
if docker ps | grep -q n8n; then
    echo "✅ n8n이 성공적으로 실행되었습니다!"
    echo ""
    echo "📊 n8n 접속 정보:"
    echo "  - 웹 인터페이스: http://localhost:5678"
    echo "  - 사용자명: admin"
    echo "  - 비밀번호: password"
    echo ""
    echo "🔧 유용한 명령어:"
    echo "  - 서비스 중지: docker compose -f docker-compose-n8n.yml down"
    echo "  - 로그 확인: docker compose -f docker-compose-n8n.yml logs n8n"
    echo "  - 컨테이너 재시작: docker compose -f docker-compose-n8n.yml restart"
    echo ""
    echo "🐍 Python 스크립트 실행:"
    echo "  python n8n_test.py"
    echo ""
    echo "🌐 n8n 웹 인터페이스 접속:"
    echo "  브라우저에서 http://localhost:5678 으로 접속하여 n8n을 사용할 수 있습니다."
    echo ""
    echo "📚 n8n 주요 기능:"
    echo "  - 워크플로우 자동화"
    echo "  - API 통합"
    echo "  - 데이터 처리"
    echo "  - 스케줄링"
    echo "  - 웹훅 처리"
else
    echo "❌ n8n 실행에 실패했습니다."
    echo "로그를 확인해보세요: docker compose -f docker-compose-n8n.yml logs"
fi 
# Kafka → n8n → ClickHouse 자동화 시스템

메시지 수신부터 처리, 저장까지의 전체 파이프라인을 테스트하는 프로젝트입니다.

## 🎯 목표

```
Kafka → (n8n 트리거) → (메시지 처리) → ClickHouse 저장
                            │
                            └→ 다른 앱 실행 (Slack, DB, API 등)
```

## 📋 구성 요소

### 1. Kafka → n8n 연동
- **메시지 전달**: Kafka 프로듀서로 메시지 전송
- **메시지 소비**: n8n 웹훅으로 메시지 수신 및 처리

![Kafka to n8n Flow](https://github.com/user-attachments/assets/a2b91f2b-2989-4492-a5d0-0e70c2b2836a)

### 2. n8n 자동화 워크플로우
- **트리거 설정**: Kafka 메시지 수신 시 자동 실행
- **워크플로우 구성**: 메시지 처리 및 다음 단계 연결

![n8n Workflow](https://github.com/user-attachments/assets/dbe155ef-5043-4e33-9e13-653cace84064)

### 3. Gmail 자동 메일 발송
- **대상 이메일**: 지정된 주소로 자동 메일 전송
- **메일 내용**: Kafka 메시지 기반 동적 내용 생성

![Gmail Integration](https://github.com/user-attachments/assets/47b12c65-8511-4510-94ae-a5c88a47476d)

### 4. AI 기반 메일 요약 (ChatGPT-4o)
- **메일 수신**: Gmail에서 메일 자동 수집
- **AI 요약**: OpenAI를 통한 메일 내용 요약 및 가독성 개선
- **용도**: 뉴스 기사나 중요 메일의 핵심 내용을 빠르게 파악

![AI Mail Summary](https://github.com/user-attachments/assets/e9e074f4-ce9f-426c-a8a6-41889e968055)

## 🚀 기술 스택

- **메시지 큐**: Apache Kafka
- **자동화 플랫폼**: n8n
- **데이터베이스**: ClickHouse
- **AI 서비스**: OpenAI (ChatGPT-4o)
- **이메일**: Gmail API

## 📁 프로젝트 구조

```
automation/
├── main.py              # 메인 실행 파일
├── click_house.py       # ClickHouse 연결 및 테스트
├── kafka_test.py        # Kafka 예제 코드
├── ml_clustering.py     # ML 클러스터링 예제
└── venv/               # Python 가상환경
```

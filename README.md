# 클릭하우스 데이터베이스 실행 가이드

이 프로젝트는 클릭하우스 데이터베이스를 쉽게 실행하고 사용할 수 있도록 도와줍니다.

## 📋 사전 요구사항

- Docker
- Docker Compose
- Python 3.7+
- clickhouse-connect 라이브러리

## 🚀 빠른 시작

### 1. 의존성 설치

```bash
# 가상환경 활성화 (이미 생성되어 있음)
source venv/bin/activate

# 필요한 Python 패키지 설치
pip install clickhouse-connect
```

### 2. 클릭하우스 실행

```bash
# 실행 스크립트에 실행 권한 부여
chmod +x run_clickhouse.sh

# 클릭하우스 실행
./run_clickhouse.sh
```

또는 직접 Docker Compose 사용:

```bash
docker-compose up -d
```

### 3. Python 스크립트 실행

```bash
python click_house.py
```

## 🔧 수동 설치 방법

### Docker 없이 직접 설치 (macOS)

```bash
# Homebrew를 사용한 설치
brew install clickhouse

# 클릭하우스 서버 시작
clickhouse-server --config-file=/usr/local/etc/clickhouse-server/config.xml
```

### Docker를 사용한 설치

```bash
# 클릭하우스 이미지 다운로드
docker pull clickhouse/clickhouse-server:latest

# 클릭하우스 컨테이너 실행
docker run -d \
  --name clickhouse-server \
  -p 8123:8123 \
  -p 9000:9000 \
  clickhouse/clickhouse-server:latest
```

## 📊 접속 정보

- **HTTP 인터페이스**: http://localhost:8123
- **Native 포트**: localhost:9000
- **기본 사용자**: default
- **기본 비밀번호**: (없음)

## 🐍 Python 연결 예제

```python
import clickhouse_connect

# 클릭하우스에 연결
client = clickhouse_connect.get_client(
    host='localhost',
    port=8123,
    username='default',
    password='',
    database='default'
)

# 쿼리 실행
result = client.query('SELECT version()')
print(result.result_rows[0][0])
```

## 🔍 유용한 명령어

### Docker 관련

```bash
# 클릭하우스 서버 시작
docker-compose up -d

# 서버 중지
docker-compose down

# 로그 확인
docker-compose logs clickhouse

# 컨테이너 재시작
docker-compose restart

# 컨테이너 상태 확인
docker ps
```

### 클릭하우스 클라이언트

```bash
# 클릭하우스 클라이언트 실행
docker exec -it clickhouse-server clickhouse-client

# 또는 직접 설치한 경우
clickhouse-client
```

## 📁 프로젝트 구조

```
cafe24/
├── click_house.py          # 클릭하우스 연결 및 테스트 스크립트
├── docker-compose.yml      # Docker Compose 설정
├── run_clickhouse.sh       # 실행 스크립트
├── README.md              # 이 파일
└── venv/                  # Python 가상환경
```

## 🛠️ 문제 해결

### 포트 충돌 문제

만약 8123 또는 9000 포트가 이미 사용 중이라면:

```bash
# 사용 중인 포트 확인
lsof -i :8123
lsof -i :9000

# docker-compose.yml에서 포트 변경
# ports:
#   - "8124:8123"   # 8124로 변경
#   - "9001:9000"   # 9001로 변경
```

### 권한 문제

```bash
# 실행 스크립트에 실행 권한 부여
chmod +x run_clickhouse.sh
```

### 메모리 부족 문제

클릭하우스는 많은 메모리를 사용할 수 있습니다. Docker 설정에서 메모리 제한을 조정하세요:

```yaml
# docker-compose.yml에 추가
services:
  clickhouse:
    # ... 기존 설정 ...
    deploy:
      resources:
        limits:
          memory: 4G
```

## 📚 추가 리소스

- [클릭하우스 공식 문서](https://clickhouse.com/docs/)
- [클릭하우스 Python 드라이버](https://github.com/ClickHouse/clickhouse-connect-python)
- [Docker Hub 클릭하우스 이미지](https://hub.docker.com/r/clickhouse/clickhouse-server) 
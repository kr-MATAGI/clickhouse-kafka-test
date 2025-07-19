#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
클릭하우스 데이터베이스 연결 및 실행 예제
"""

import clickhouse_connect
from clickhouse_connect.driver.client import Client


def connect_to_clickhouse():
    """클릭하우스 데이터베이스에 연결"""
    try:
        # 클릭하우스 서버에 연결
        client = clickhouse_connect.get_client(
            host="localhost",  # 클릭하우스 서버 호스트
            port=8123,  # HTTP 포트
            username="default",  # 기본 사용자명
            password="",  # 기본 비밀번호 (설정하지 않은 경우)
            database="default",  # 기본 데이터베이스
        )
        print("✅ 클릭하우스 데이터베이스에 성공적으로 연결되었습니다!")
        return client
    except Exception as e:
        print(f"❌ 클릭하우스 연결 실패: {e}")
        return None


def test_connection(client):
    """연결 테스트 및 기본 쿼리 실행"""
    if client is None:
        return

    try:
        # 서버 버전 확인
        result = client.query("SELECT version()")
        print(f"📊 클릭하우스 버전: {result.result_rows[0][0]}")

        # 데이터베이스 목록 조회
        result = client.query("SHOW DATABASES")
        print("📋 사용 가능한 데이터베이스:")
        for row in result.result_rows:
            print(f"  - {row[0]}")

    except Exception as e:
        print(f"❌ 쿼리 실행 실패: {e}")


def create_sample_table(client):
    """샘플 테이블 생성"""
    if client is None:
        return

    try:
        # 샘플 테이블 생성
        create_table_query = """
        CREATE TABLE IF NOT EXISTS sample_table (
            id UInt32,
            name String,
            value Float64,
            created_at DateTime
        ) ENGINE = MergeTree()
        ORDER BY id
        """

        client.command(create_table_query)
        print("✅ 샘플 테이블이 성공적으로 생성되었습니다!")

        # 샘플 데이터 삽입
        from datetime import datetime

        sample_data = [
            (1, "Item 1", 10.5, datetime(2024, 1, 1, 10, 0, 0)),
            (2, "Item 2", 20.3, datetime(2024, 1, 2, 11, 0, 0)),
            (3, "Item 3", 15.7, datetime(2024, 1, 3, 12, 0, 0)),
        ]

        client.insert(
            "sample_table",
            sample_data,
            column_names=["id", "name", "value", "created_at"],
        )
        print("✅ 샘플 데이터가 성공적으로 삽입되었습니다!")

        # 데이터 조회
        result = client.query("SELECT * FROM sample_table ORDER BY id")
        print("📊 샘플 테이블 데이터:")
        for row in result.result_rows:
            print(f"  ID: {row[0]}, Name: {row[1]}, Value: {row[2]}, Created: {row[3]}")

    except Exception as e:
        print(f"❌ 테이블 생성 실패: {e}")


def main():
    """메인 함수"""
    print("🚀 클릭하우스 데이터베이스 연결 시작...")

    # 클릭하우스에 연결
    client = connect_to_clickhouse()

    if client:
        # 연결 테스트
        test_connection(client)

        # 샘플 테이블 생성 및 데이터 조작
        create_sample_table(client)

        # 연결 종료
        client.close()
        print("👋 클릭하우스 연결이 종료되었습니다.")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
n8n Python 연동 예제
n8n 워크플로우를 Python에서 호출하고 관리하는 방법들
"""

import requests
import json
import time
from typing import Dict, Any, List
from datetime import datetime


class N8nClient:
    """n8n API 클라이언트"""

    def __init__(self, base_url: str = "http://localhost:5678", api_key: str = None):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.session = requests.Session()

        if api_key:
            self.session.headers.update({"X-N8N-API-KEY": api_key})

    def get_workflows(self) -> List[Dict[str, Any]]:
        """워크플로우 목록 조회"""
        try:
            response = self.session.get(f"{self.base_url}/api/v1/workflows")
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"❌ 워크플로우 목록 조회 실패: {e}")
            return []

    def get_workflow(self, workflow_id: str) -> Dict[str, Any]:
        """특정 워크플로우 조회"""
        try:
            response = self.session.get(
                f"{self.base_url}/api/v1/workflows/{workflow_id}"
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"❌ 워크플로우 조회 실패: {e}")
            return {}

    def activate_workflow(self, workflow_id: str) -> bool:
        """워크플로우 활성화"""
        try:
            response = self.session.post(
                f"{self.base_url}/api/v1/workflows/{workflow_id}/activate"
            )
            response.raise_for_status()
            print(f"✅ 워크플로우 {workflow_id} 활성화 완료")
            return True
        except requests.exceptions.RequestException as e:
            print(f"❌ 워크플로우 활성화 실패: {e}")
            return False

    def deactivate_workflow(self, workflow_id: str) -> bool:
        """워크플로우 비활성화"""
        try:
            response = self.session.post(
                f"{self.base_url}/api/v1/workflows/{workflow_id}/deactivate"
            )
            response.raise_for_status()
            print(f"✅ 워크플로우 {workflow_id} 비활성화 완료")
            return True
        except requests.exceptions.RequestException as e:
            print(f"❌ 워크플로우 비활성화 실패: {e}")
            return False

    def trigger_workflow(
        self, workflow_id: str, data: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """워크플로우 수동 트리거"""
        try:
            payload = data or {}
            response = self.session.post(
                f"{self.base_url}/api/v1/workflows/{workflow_id}/trigger", json=payload
            )
            response.raise_for_status()
            result = response.json()
            print(f"✅ 워크플로우 {workflow_id} 트리거 완료")
            return result
        except requests.exceptions.RequestException as e:
            print(f"❌ 워크플로우 트리거 실패: {e}")
            return {}

    def get_executions(
        self, workflow_id: str = None, limit: int = 10
    ) -> List[Dict[str, Any]]:
        """실행 기록 조회"""
        try:
            url = f"{self.base_url}/api/v1/executions"
            params = {"limit": limit}
            if workflow_id:
                params["workflowId"] = workflow_id

            response = self.session.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"❌ 실행 기록 조회 실패: {e}")
            return []


def create_simple_webhook_workflow():
    """간단한 웹훅 워크플로우 생성 예제"""
    workflow = {
        "name": "Python Test Workflow",
        "nodes": [
            {
                "id": "webhook",
                "name": "Webhook",
                "type": "n8n-nodes-base.webhook",
                "typeVersion": 1,
                "position": [240, 300],
                "parameters": {
                    "httpMethod": "POST",
                    "path": "python-test",
                    "responseMode": "responseNode",
                },
            },
            {
                "id": "set",
                "name": "Set",
                "type": "n8n-nodes-base.set",
                "typeVersion": 3.2,
                "position": [460, 300],
                "parameters": {
                    "values": {
                        "string": [
                            {"name": "message", "value": "Hello from Python!"},
                            {"name": "timestamp", "value": "={{ $now }}"},
                        ]
                    }
                },
            },
        ],
        "connections": {
            "Webhook": {"main": [[{"node": "Set", "type": "main", "index": 0}]]}
        },
    }
    return workflow


def create_data_processing_workflow():
    """데이터 처리 워크플로우 예제"""
    workflow = {
        "name": "Data Processing Workflow",
        "nodes": [
            {
                "id": "webhook",
                "name": "Data Input",
                "type": "n8n-nodes-base.webhook",
                "typeVersion": 1,
                "position": [240, 300],
                "parameters": {
                    "httpMethod": "POST",
                    "path": "data-process",
                    "responseMode": "responseNode",
                },
            },
            {
                "id": "filter",
                "name": "Filter Data",
                "type": "n8n-nodes-base.if",
                "typeVersion": 2,
                "position": [460, 300],
                "parameters": {
                    "conditions": {
                        "string": [
                            {
                                "value1": "={{ $json.value }}",
                                "operation": "larger",
                                "value2": 10,
                            }
                        ]
                    }
                },
            },
            {
                "id": "set",
                "name": "Process Result",
                "type": "n8n-nodes-base.set",
                "typeVersion": 3.2,
                "position": [680, 300],
                "parameters": {
                    "values": {
                        "string": [
                            {"name": "processed", "value": "true"},
                            {"name": "original_value", "value": "={{ $json.value }}"},
                        ]
                    }
                },
            },
        ],
        "connections": {
            "Data Input": {
                "main": [[{"node": "Filter Data", "type": "main", "index": 0}]]
            },
            "Filter Data": {
                "main": [[{"node": "Process Result", "type": "main", "index": 0}]]
            },
        },
    }
    return workflow


def test_n8n_integration():
    """n8n 통합 테스트"""
    print("🚀 n8n Python 통합 테스트 시작")
    print("=" * 50)

    # n8n 클라이언트 생성
    n8n = N8nClient()

    # n8n 서버 연결 확인
    try:
        workflows = n8n.get_workflows()
        print(f"✅ n8n 서버 연결 성공")
        print(f"📋 총 {len(workflows)}개의 워크플로우 발견")

        # 워크플로우 목록 출력
        for workflow in workflows:
            print(
                f"  - {workflow.get('name', 'Unknown')} (ID: {workflow.get('id', 'N/A')})"
            )
            print(f"    활성화 상태: {workflow.get('active', False)}")

    except Exception as e:
        print(f"❌ n8n 서버 연결 실패: {e}")
        print(
            "💡 n8n 서버가 실행 중인지 확인하세요: docker run -it --rm --name n8n -p 5678:5678 n8nio/n8n"
        )
        return

    # 웹훅 테스트
    print("\n🔗 웹훅 테스트")
    print("-" * 30)

    # 웹훅 URL 예제
    webhook_url = "http://localhost:5678/webhook/python-test"

    test_data = {
        "message": "Hello from Python!",
        "timestamp": datetime.now().isoformat(),
        "value": 42,
    }

    try:
        response = requests.post(webhook_url, json=test_data)
        print(f"📤 웹훅 전송 결과: {response.status_code}")
        if response.status_code == 200:
            print(f"📥 응답: {response.json()}")
    except Exception as e:
        print(f"❌ 웹훅 테스트 실패: {e}")


def create_n8n_workflow_from_python():
    """Python에서 n8n 워크플로우 생성"""
    print("\n🔧 Python에서 n8n 워크플로우 생성")
    print("-" * 40)

    # 간단한 워크플로우 생성
    workflow = create_simple_webhook_workflow()

    print("📋 생성된 워크플로우 구조:")
    print(json.dumps(workflow, indent=2, ensure_ascii=False))

    return workflow


def monitor_workflow_executions():
    """워크플로우 실행 모니터링"""
    print("\n📊 워크플로우 실행 모니터링")
    print("-" * 40)

    n8n = N8nClient()

    # 최근 실행 기록 조회
    executions = n8n.get_executions(limit=5)

    if executions:
        print(f"📈 최근 {len(executions)}개의 실행 기록:")
        for execution in executions:
            workflow_name = execution.get("workflowName", "Unknown")
            status = execution.get("status", "Unknown")
            started_at = execution.get("startedAt", "Unknown")

            print(f"  - {workflow_name}: {status} (시작: {started_at})")
    else:
        print("📭 실행 기록이 없습니다.")


def main():
    """메인 함수"""
    print("🎯 n8n Python 연동 예제")
    print("=" * 60)

    # 1. n8n 통합 테스트
    test_n8n_integration()

    # 2. 워크플로우 생성 예제
    create_n8n_workflow_from_python()

    # 3. 실행 모니터링
    monitor_workflow_executions()

    print("\n✅ n8n Python 연동 예제 완료!")
    print("\n💡 다음 단계:")
    print("1. n8n 서버 실행: docker run -it --rm --name n8n -p 5678:5678 n8nio/n8n")
    print("2. 웹 브라우저에서 http://localhost:5678 접속")
    print("3. 워크플로우 생성 및 테스트")
    print("4. Python에서 API 호출하여 워크플로우 제어")


if __name__ == "__main__":
    main()

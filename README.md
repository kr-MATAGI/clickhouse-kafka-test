Kafka → (n8n 트리거) → (메시지 처리) → ClickHouse 저장
                            │
                            └→ 다른 앱 실행 (Slack, DB, API 등)


이런거 목표로 한번 테스트만 되는거 만들어보기


### kafka -> n8n

- 메시지 전달 -> 소비
  
<img width="510" height="362" alt="image" src="https://github.com/user-attachments/assets/a2b91f2b-2989-4492-a5d0-0e70c2b2836a" />


- n8n에서 자동화 셋팅
<img width="1755" height="942" alt="image" src="https://github.com/user-attachments/assets/dbe155ef-5043-4e33-9e13-653cace84064" />


### n8n send to mail (GMail)

- 대상 이메일로 내용 보내기
<img width="844" height="327" alt="image" src="https://github.com/user-attachments/assets/47b12c65-8511-4510-94ae-a5c88a47476d" />

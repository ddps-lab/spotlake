from sps_lambda_handler import lambda_handler

# EventBridge 이벤트를 시뮬레이션하는 함수
def simulate_event(action, event_time):
    return {
        "action": action,  # 액션 종류 (first_time 또는 collect_score)
        "time": event_time  # UTC 시간 (ISO 8601 형식)
    }

# 시뮬레이션할 시간과 액션 목록 정의
simulation_times = [
    ("first_time", "2025-01-28T00:00:00Z"),  # 0:00 호출 시뮬레이션
    ("collect_score", "2025-01-28T00:10:00Z"),  # 0:10 호출 시뮬레이션
    ("collect_score", "2025-01-28T00:20:00Z"),  # 0:20 호출 시뮬레이션
]

# 시뮬레이션 시간을 반복하며 lambda_handler 호출
for action, event_time in simulation_times:
    print(f"\nSimulating action='{action}' with time='{event_time}'")
    event = simulate_event(action=action, event_time=event_time)  # 이벤트 생성
    response = lambda_handler(event, None)  # Lambda 함수 호출
    print(f"Response: {response}")  # 결과 출력
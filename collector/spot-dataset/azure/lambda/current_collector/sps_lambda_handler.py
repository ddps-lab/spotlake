import load_sps
import datetime

# desired_count의 순환 목록 정의
desired_counts = [5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 1]


def lambda_handler(event, _):
    # EventBridge 규칙에서 전달된 action 매개변수 가져오기
    action = event.get("action", "default")

    if action == "first_time":
        # EventBridge에서 제공한 호출 시간 가져오기
        event_time_utc = event['time']  # ISO 8601 형식 (예: "2025-01-28T00:00:00Z")
        event_time = datetime.datetime.strptime(event_time_utc, "%Y-%m-%dT%H:%M:%SZ")
        formatted_time = event_time.strftime("%Y-%m-%d_00:00")

        print(f"Calling collect_spot_placement_score_first_time with desired_count=1 and time={formatted_time}")
        load_sps.collect_spot_placement_score_first_time(desired_count=1, request_time=formatted_time)

    elif action == "collect_score":
        # 0:10부터 10분 간격으로 호출되는 로직 처리
        event_time_utc = event['time']  # EventBridge에서 제공한 호출 시간 가져오기
        event_time = datetime.datetime.strptime(event_time_utc, "%Y-%m-%dT%H:%M:%SZ")
        formatted_time = event_time.strftime("%Y-%m-%d_%H:%M")

        # 호출 시간에 따라 desired_count 계산
        current_minute = event_time.minute  # 현재 분 가져오기
        index = (current_minute // 10 - 1) % len(desired_counts)  # 분을 기반으로 인덱스 계산
        desired_count = desired_counts[index]

        print(f"Calling collect_spot_placement_score with desired_count={desired_count} and time={formatted_time}")
        load_sps.collect_spot_placement_score(desired_count=desired_count, request_time=formatted_time)

    else:
        # action이 잘못된 경우 로깅 및 응답 반환
        print("Invalid action provided in the event.")
        return {
            "statusCode": 400,
            "body": "Invalid action"
        }

    # 성공적으로 실행된 결과 반환
    return {
        "statusCode": 200,
        "body": f"Action '{action}' executed successfully"
    }
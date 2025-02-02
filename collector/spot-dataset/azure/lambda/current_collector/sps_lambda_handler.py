import load_sps
from datetime import datetime, timezone, timedelta

# desired_count의 순환 목록 정의
desired_counts = [1, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50]

def lambda_handler(event, _):
    # EventBridge 규칙에서 전달된 action 매개변수 가져오기
    action = event.get("action", "default")
    event_time_utc = event.get("time", "default")  # EventBridge에서 전달된 UTC 시간 문자열

    # UTC 시간을 KST로 변환
    event_time_utc = datetime.strptime(event_time_utc, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    korea_time = event_time_utc + timedelta(hours=9)
    formatted_time = korea_time.strftime("%Y-%m-%d_%H:%M")

    if action == "First_Time":
        print(f"Calling collect_spot_placement_score_first_time with desired_count: [1] and time: [{formatted_time}]")
        load_sps.collect_spot_placement_score_first_time(desired_count=1, collect_time=formatted_time)

    elif action == "Every_10Min":
        total_10min_intervals = (korea_time.hour * 6) + (korea_time.minute // 10)
        index = total_10min_intervals % len(desired_counts)
        desired_count = desired_counts[index]

        print(f"Calling collect_spot_placement_score with desired_count: [{desired_count}] and time: [{formatted_time}]")
        load_sps.collect_spot_placement_score(desired_count=desired_count, collect_time=formatted_time)

    else:
        print("Invalid action provided in the event.")
        return {
            "statusCode": 400,
            "body": "Invalid action"
        }

    return {
        "statusCode": 200,
        "body": f"Action '{action}' executed successfully"
    }
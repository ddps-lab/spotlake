import load_sps
from datetime import datetime, timezone, timedelta

import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

logger.info("----------Lambda Start-----------")

# desired_count의 순환 목록 정의
desired_counts = [5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 1]

def lambda_handler(event, _):
    # EventBridge 규칙에서 전달된 action 매개변수 가져오기
    action = event.get("action", "default")
    event_time_utc  = event.get("time", "default")  # EventBridge에서 전달된 UTC 시간 문자열

    event_time_utc = datetime.strptime(event_time_utc, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    korea_time = event_time_utc + timedelta(hours=9)  # 转换到 KST
    formatted_time = korea_time.strftime("%Y-%m-%d_%H:%M")

    if action == "First_Time":
        print(f"Calling collect_spot_placement_score_first_time with desired_count: [1] and time: [{formatted_time}]")
        load_sps.collect_spot_placement_score_first_time(desired_count=1, collect_time=formatted_time)

    elif action == "Every_10Min":
        # 호출 시간에 따라 desired_count 계산
        current_minute = korea_time.minute  # 현재 분 가져오기
        index = (current_minute // 10 - 1) % len(desired_counts)  # 분을 기반으로 인덱스 계산
        desired_count = desired_counts[index]

        print(f"Calling collect_spot_placement_score with desired_count: [{desired_count}] and time: [{formatted_time}]")
        load_sps.collect_spot_placement_score(desired_count=desired_count, collect_time=formatted_time)

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
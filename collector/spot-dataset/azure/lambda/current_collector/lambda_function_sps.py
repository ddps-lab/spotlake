import load_sps
import slack_msg_sender
from sps_module import sps_shared_resources
from datetime import datetime
from upload_data import update_latest_sps, save_raw_sps



def lambda_handler(event, _):
    # try:
    # EventBridge 규칙에서 전달된 action 매개변수 가져오기
    action = event.get("action", "default")
    event_time_utc = event.get("time", "default")  # EventBridge에서 전달된 UTC 시간 문자열
    event_time_utc = datetime.strptime(event_time_utc, "%Y-%m-%dT%H:%M:%SZ")
    desired_count = sps_shared_resources.time_desired_count_map.get(event_time_utc.strftime("%H:%M"), 1)

    # Event Bridge 에서 0:00의 호출은 First_Time으로 오고, 기타는 Every_10Min로 옵니다.
    if action == "First_Time":
        print(f"Calling collect_spot_placement_score_first_time with desired_count: [{desired_count}] and time: [{event_time_utc}]")
        sps_res_df = load_sps.collect_spot_placement_score_first_time(desired_count=desired_count)
        update_latest_sps(sps_res_df, event_time_utc)
        save_raw_sps(sps_res_df, event_time_utc)

    elif action == "Every_10Min":
        print(f"Calling collect_spot_placement_score with desired_count: [{desired_count}] and time: [{event_time_utc}]")
        sps_res_df = load_sps.collect_spot_placement_score(desired_count=desired_count)
        update_latest_sps(sps_res_df, event_time_utc)
        save_raw_sps(sps_res_df, event_time_utc)

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
    # except Exception as e:
    #     result_msg = """AZURE SPS MODULE EXCEPTION! TEST TEST TEST \n %s""" % (e)
    #     data = {'text': result_msg}
    #     slack_msg_sender.send_slack_message(result_msg)
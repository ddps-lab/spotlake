import load_sps
from sps_module import sps_shared_resources
from datetime import datetime
from utils.upload_data import update_latest_sps, save_raw_sps
from utils.pub_service import send_slack_message

def lambda_handler(event, _):
    try:
        # EventBridge 규칙에서 전달된 action 매개변수 가져오기
        action = event.get("action", "default") # EventBridge에서 전달된 UTC 시간 문자열
        event_time_utc = event.get("time", "default")
        event_time_utc = datetime.strptime(event_time_utc, "%Y-%m-%dT%H:%M:%SZ")
        desired_count = sps_shared_resources.time_desired_count_map.get(event_time_utc.strftime("%H:%M"), 1)

        print(f"Lambda triggered: action={action}, event_time_utc={event_time_utc}, desired_count={desired_count}")

        # Event Bridge 에서 0:00의 호출은 First_Time으로 오고, 기타는 Every_10Min로 옵니다.
        if action == "First_Time":
            print(f"Executing: collect_spot_placement_score_first_time (desired_count={desired_count})")
            sps_res_df = load_sps.collect_spot_placement_score_first_time(desired_count=desired_count)

        elif action == "Every_10Min":
            if event_time_utc.hour == 15 and event_time_utc.minute == 0:
                return handle_response(200, f"Action '{action}' executed successfully. The scheduled time (UTC 15:00, KST 00:00) has been skipped.")

            print(f"Executing: collect_spot_placement_score (desired_count={desired_count})")
            sps_res_df = load_sps.collect_spot_placement_score(desired_count=desired_count)

        else:
            return handle_response(400, f"Invalid action: '{action}'. Time: {event_time_utc}")

        assert sps_res_df is not None and handle_res_df(sps_res_df, event_time_utc)
        return handle_response(200, f"Action '{action}' executed successfully")

    except Exception as e:
        error_msg = f"AZURE SPS MODULE EXCEPTION!\n Error: {e}"
        send_slack_message(error_msg)
        return handle_response(400, f"Action '{action}' executed failed. Time: {event_time_utc}.", error_msg)


def handle_res_df(sps_res_df, event_time_utc):
    update_result = update_latest_sps(sps_res_df, event_time_utc)
    save_result = save_raw_sps(sps_res_df, event_time_utc)
    return update_result and save_result

def handle_response(status_code, body, error_message=None):
    response = {"statusCode": status_code, "body": body}
    if error_message:
        response["error_message"] = error_message
    print(f"Response: {response}")
    return response
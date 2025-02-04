import load_sps
import requests
import inspect
import os
from sps_module import sps_shared_resources
from datetime import datetime
from upload_data import update_latest_sps, save_raw_sps

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
        error_msg = f"TEST____TEST____AZURE SPS MODULE EXCEPTION!\n {e}"
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

def send_slack_message(msg):
    url = os.environ.get('error_notification_slack_webhook_url')

    module_name = inspect.stack()[1][1]
    line_no = inspect.stack()[1][2]
    function_name = inspect.stack()[1][3]

    message = f"File \"{module_name}\", line {line_no}, in {function_name} :\n{msg}"
    slack_data = {
        "text": message
    }
    requests.post(url, json=slack_data)
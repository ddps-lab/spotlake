import logging
import load_sps
from datetime import datetime
from sps_module import sps_shared_resources
from utils.upload_data import update_latest_sps, save_raw_sps
from utils.pub_service import send_slack_message

logger = logging.getLogger()
logger.setLevel(logging.INFO)

FIRST_TIME_ACTION = "First_Time"  # 첫 실행 액션
EVERY_10MIN_ACTION = "Every_10Min"  # 10분마다 실행 액션
UTC_1500_TIME = "15:00"  # UTC 15:00 (KST 00:00)

def lambda_handler(event, _):
    action = event.get("action")
    event_time_utc = event.get("time")
    try:
        if not action or not event_time_utc:
            raise ValueError("Invalid event info: action or time is missing")

        event_time_utc = datetime.strptime(event_time_utc, "%Y-%m-%dT%H:%M:%SZ")
        desired_count = sps_shared_resources.time_desired_count_map.get(event_time_utc.strftime("%H:%M"), 1)

        logger.info(f"Lambda triggered: action={action}, event_time_utc={event_time_utc}, desired_count={desired_count}")

        if action == FIRST_TIME_ACTION:
            sps_res_df = load_sps.collect_spot_placement_score_first_time(desired_count=desired_count)

        elif action == EVERY_10MIN_ACTION:
            # UTC 15:00 (KST 00:00)인 경우 실행 건너뛰기
            if event_time_utc.strftime("%H:%M") == UTC_1500_TIME:
                logger.info("Skipping scheduled time (UTC 15:00, KST 00:00)")
                return handle_response(200, "Executed successfully. Scheduled time skipped.", action, event_time_utc)

            sps_res_df = load_sps.collect_spot_placement_score(desired_count=desired_count)
        else:
            raise ValueError(f"Invalid lambda action.")

        # SPS 데이터 처리
        if sps_res_df is None:
            raise ValueError("sps_res_df is None")
        if not handle_res_df(sps_res_df, event_time_utc):
            raise RuntimeError("Failed to update or save SPS data")

        return handle_response(200, "Executed Successfully!", action, event_time_utc)

    except Exception as e:
        error_msg = f"Unexpected error: {e}"
        logger.error(error_msg)
        send_slack_message(f"AZURE SPS MODULE EXCEPTION!\n{error_msg}")
        return handle_response(500, "Execute Failed!", action, event_time_utc, str(e))


def handle_res_df(sps_res_df, event_time_utc):
    try:
        update_result = update_latest_sps(sps_res_df, event_time_utc)
        save_result = save_raw_sps(sps_res_df, event_time_utc)
        return update_result and save_result

    except Exception as e:
        logger.error(f"Error in handle_res_df function: {e}")
        return False

def handle_response(status_code, body, action, time, error_message=None):
    response = {
        "statusCode": status_code,
        "body": body,
        "action": action,
        "time": time
    }
    if error_message:
        response["error_message"] = error_message

    logger.info(f"Response: {response}")
    return response
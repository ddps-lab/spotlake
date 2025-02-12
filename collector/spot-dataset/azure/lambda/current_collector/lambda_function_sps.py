import load_sps
import pandas as pd
from datetime import datetime
from sps_module import sps_shared_resources
from utils.merge_df import merge_price_eviction_sps_df
from utils.upload_data import update_latest_sps, save_raw_sps
from utils.pub_service import send_slack_message, logger, S3, AZURE_CONST

FIRST_TIME_ACTION = "First_Time"  # 첫 실행 액션
EVERY_10MIN_ACTION = "Every_10Min"  # 10분마다 실행 액션
UTC_1500_TIME = "15:00"  # UTC 15:00 (KST 00:00)

def lambda_handler(event, _):
    action = event.get("action")
    event_id = event.get("id")
    event_time_utc = event.get("time")
    event_time_utc_datetime = datetime.strptime(event_time_utc, "%Y-%m-%dT%H:%M:%SZ")

    try:
        if not action or not event_time_utc:
            raise ValueError("Invalid event info: action or time is missing")

        desired_count = sps_shared_resources.time_desired_count_map.get(event_time_utc_datetime.strftime("%H:%M"), 1)

        logger.info(f"Lambda triggered: action: {action}, event_time: {datetime.strftime(event_time_utc_datetime, '%Y-%m-%d %H:%M:%S')}, desired_count: {desired_count}")

        if action == FIRST_TIME_ACTION:
            sps_res_df = load_sps.collect_spot_placement_score_first_time(desired_count=desired_count)

        elif action == EVERY_10MIN_ACTION:
            # UTC 15:00 (KST 00:00)인 경우 실행 건너뛰기
            if event_time_utc_datetime.strftime("%H:%M") == UTC_1500_TIME:
                logger.info("Skipping scheduled time (UTC 15:00, KST 00:00)")
                return handle_response(200, "Executed successfully. Scheduled time skipped.", action, event_time_utc_datetime)

            sps_res_df = load_sps.collect_spot_placement_score(desired_count=desired_count)

        else:
            raise ValueError(f"Invalid lambda action.")


        if sps_res_df is None: raise ValueError("sps_res_df is None")

        if not handle_res_df(sps_res_df, event_time_utc_datetime):
            raise RuntimeError("Failed to handle_res_df")

        return handle_response(200, "Executed Successfully!", action, event_time_utc_datetime)

    except Exception as e:
        error_msg = f"Unexpected error: {e}"
        logger.error(error_msg)
        send_slack_message(f"AZURE SPS MODULE EXCEPTION!\n{error_msg}\nEvent_id: {event_id}")
        return handle_response(500, "Execute Failed!", action, event_time_utc_datetime, str(e))


def handle_res_df(sps_res_df, time_datetime):
    try:
        sps_res_df['time'] = time_datetime.strftime("%Y-%m-%d %H:%M:%S")
        sps_res_df['AvailabilityZone'] = sps_res_df['AvailabilityZone'].where(pd.notna(sps_res_df['AvailabilityZone']), None)

        # price_if_df = S3.read_file(AZURE_CONST.S3_LATEST_PRICE_IF_GZIP_SAVE_PATH, 'pkl.gz')
        # if price_if_df is None: raise ValueError("price_if_df is None")
        price_if_df = pd.DataFrame(S3.read_file(AZURE_CONST.S3_LATEST_DATA_SAVE_PATH, 'json'))
        price_eviction_sps_df = merge_price_eviction_sps_df(price_if_df, sps_res_df)

        if update_latest_sps(price_eviction_sps_df) and save_raw_sps(price_eviction_sps_df, time_datetime):
            logger.info(f"Successfully merge the price/if/sps df, and update_latest_result, save_raw!")
            return True

    except Exception as e:
        logger.error(f"Error in handle_res_df function: {e}")
        return False

def handle_response(status_code, body, action, time_datetime, error_message=None):
    response = {
        "statusCode": status_code,
        "body": body,
        "action": action,
        "time": datetime.strftime(time_datetime, '%Y-%m-%d %H:%M:%S')
    }
    if error_message:
        response["error_message"] = error_message

    logger.info(f"Response: {response}")
    return response
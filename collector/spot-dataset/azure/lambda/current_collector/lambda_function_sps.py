import load_sps
import pandas as pd
import traceback
from datetime import datetime
from sps_module import sps_shared_resources
from utils.merge_df import merge_price_eviction_sps_df
from utils.upload_data import update_latest_sps, save_raw_sps, upload_timestream, query_selector, upload_cloudwatch
from utils.compare_data import compare_sps
from utils.pub_service import send_slack_message, Logger, S3, AZURE_CONST

FIRST_TIME_ACTION = "First_Time"  # 첫 실행 액션
EVERY_10MIN_ACTION = "Every_10Min"  # 10분마다 실행 액션
UTC_1500_TIME = "15:00"  # UTC 15:00 (KST 00:00)

def lambda_handler(event, context):
    action = event.get("action")
    log_stream_id = context.log_stream_name
    event_time_utc = event.get("time")
    event_time_utc_datetime = datetime.strptime(event_time_utc, "%Y-%m-%dT%H:%M:%SZ")

    try:
        if not action or not event_time_utc:
            raise ValueError("Invalid event info: action or time is missing")

        desired_count = sps_shared_resources.time_desired_count_map.get(event_time_utc_datetime.strftime("%H:%M"), 1)

        Logger.info(f"Lambda triggered: action: {action}, event_time: {datetime.strftime(event_time_utc_datetime, '%Y-%m-%d %H:%M:%S')}, desired_count: {desired_count}")

        if action == FIRST_TIME_ACTION:
            sps_res_availability_zones_true_df, sps_res_availability_zones_false_df = load_sps.collect_spot_placement_score_first_time(desired_count=desired_count)

        elif action == EVERY_10MIN_ACTION:
            # UTC 15:00 (KST 00:00)인 경우 실행 건너뛰기
            if event_time_utc_datetime.strftime("%H:%M") == UTC_1500_TIME:
                Logger.info("Skipping scheduled time (UTC 15:00, KST 00:00)")
                return handle_response(200, "Executed successfully. Scheduled time skipped.", action, event_time_utc_datetime)

            sps_res_availability_zones_true_df, sps_res_availability_zones_false_df = load_sps.collect_spot_placement_score(desired_count=desired_count)

        else:
            raise ValueError(f"Invalid lambda action.")


        if sps_res_availability_zones_true_df is None: raise ValueError("sps_res_true_df is None")
        if sps_res_availability_zones_false_df is None: raise ValueError("sps_res_false_df is None")

        if not handle_res_df(sps_res_availability_zones_true_df, sps_res_availability_zones_false_df, event_time_utc_datetime):
            raise RuntimeError("Failed to handle_res_df")

        return handle_response(200, "Executed Successfully!", action, event_time_utc_datetime)

    except Exception as e:
        error_msg = f"Unexpected error: {e}"
        Logger.error(error_msg)
        send_slack_message(f"LOCAL_TEST_AZURE SPS MODULE EXCEPTION!\n{error_msg}\Log_stream_id: {log_stream_id}")
        return handle_response(500, "Execute Failed!", action, event_time_utc_datetime, str(e))


def handle_res_df(sps_res_true_df, sps_res_false_df, time_datetime):
    try:
        time_str = time_datetime.strftime("%Y-%m-%d %H:%M:%S")
        sps_res_true_df['time'] = time_str
        sps_res_false_df['time'] = time_str

        sps_res_true_df['AvailabilityZone'] = sps_res_true_df['AvailabilityZone'].where(pd.notna(sps_res_true_df['AvailabilityZone']), None)

        price_if_df = S3.read_file(AZURE_CONST.S3_LATEST_PRICE_IF_GZIP_SAVE_PATH, 'pkl.gz')
        if price_if_df is None:
            raise ValueError("price_if_df is None")

        success_availability_zone_true = process_zone_data(price_if_df, sps_res_true_df, time_datetime, True)
        success_availability_zone_false = process_zone_data(price_if_df, sps_res_false_df, time_datetime, False)

        if success_availability_zone_true and success_availability_zone_false:
            Logger.info("Successfully merged the price/if/sps df, process_zone_data!")
            return True
        else:
            Logger.info("Failed to merge the price/if/sps df, process_zone_data!")
            return False

    except Exception as e:
        Logger.error(f"Error in handle_res_df function: {e}")
        return False


def process_zone_data(price_if_df, sps_res_df, time_datetime, is_true_zone):
    try:
        price_eviction_sps_zone_df = merge_price_eviction_sps_df(price_if_df, sps_res_df, is_true_zone)

        if is_true_zone:
            price_eviction_sps_zone_previous_df = S3.read_file(f"{AZURE_CONST.LATEST_SPS_AVAILABILITY_ZONE_TRUE_PKL_GZIP_FILENAME}", 'pkl.gz')
            price_eviction_sps_zone_previous_df.drop(columns=['id'], inplace=True)
            workload_cols = ['InstanceTier', 'InstanceType', 'Region', 'AvailabilityZone', 'DesiredCount']
            feature_cols = ['OndemandPrice', 'SpotPrice', 'IF', 'Score', 'SPS_Update_Time']

            changed_df = None
            if price_eviction_sps_zone_previous_df is not None and not price_eviction_sps_zone_previous_df.empty:
                changed_df = compare_sps(price_eviction_sps_zone_previous_df, price_eviction_sps_zone_df, workload_cols, feature_cols)

            update_success = update_latest_sps(price_eviction_sps_zone_df, is_true_zone)
            save_success = save_raw_sps(price_eviction_sps_zone_df, time_datetime, is_true_zone)
            cloudwatch_success = upload_cloudwatch(price_eviction_sps_zone_df, time_datetime)

            if changed_df is not None and not changed_df.empty:
                query_success = query_selector(changed_df)
                timestream_success = upload_timestream(changed_df, time_datetime)
            else:
                query_success = True
                timestream_success = True

            success = all([update_success, save_success, cloudwatch_success, query_success, timestream_success])

            log_details = (
                f"update: {update_success}, save: {save_success}, cloudwatch: {cloudwatch_success}, "
                f"query: {query_success}, timestream: {timestream_success}"
            )
        else:
            update_success = update_latest_sps(price_eviction_sps_zone_df, is_true_zone)
            save_success = save_raw_sps(price_eviction_sps_zone_df, time_datetime, is_true_zone)

            success = update_success and save_success
            log_details = f"update: {update_success}, save: {save_success}"

        if not success:
            Logger.error(f"Failed: Availability Zone {is_true_zone} Processing.")
            Logger.error(log_details)
        else:
            return True

    except Exception as e:
        Logger.error(f"Error in process_zone_data function: {e}")
        Logger.error(traceback.format_exc())
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

    return response
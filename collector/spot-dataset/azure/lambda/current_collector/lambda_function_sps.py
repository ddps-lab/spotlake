import os
import load_sps
import pandas as pd
import traceback
from datetime import datetime
from sps_module import sps_shared_resources
from utils.merge_df import merge_if_saving_price_sps_df
from utils.upload_data import update_latest, save_raw, upload_timestream, query_selector, upload_cloudwatch
from utils.compare_data import compare_sps
from utils.pub_service import send_slack_message, Logger, S3, AZURE_CONST

FIRST_TIME_ACTION = "First_Time"  # 첫 실행 액션
EVERY_10MIN_ACTION = "Every_10Min"  # 10분마다 실행 액션
UTC_1500_TIME = "15:00"  # UTC 15:00 (KST 00:00)

def lambda_handler(event, context):
    action = event.get("action")
    log_stream_name = context.log_stream_name
    event_time_utc = event.get("time")
    event_time_utc_datetime = datetime.strptime(event_time_utc, "%Y-%m-%dT%H:%M:%SZ")

    try:
        if not action or not event_time_utc:
            raise ValueError("Invalid event info: action or time is missing")

        desired_count = sps_shared_resources.time_desired_count_map.get(event_time_utc_datetime.strftime("%H:%M"), 1)
        Logger.info(f"Lambda triggered: action: {action}, event_time: {datetime.strftime(event_time_utc_datetime, '%Y-%m-%d %H:%M:%S')}")

        specific_desired_counts = [specific_desired_count.strip() for specific_desired_count in os.environ.get('specific_desired_counts').split(",") if specific_desired_count.strip()]
        specific_instance_types = [specific_instance_type.strip() for specific_instance_type in os.environ.get('specific_instance_types').split(",") if specific_instance_type.strip()]

        if action == FIRST_TIME_ACTION:
            sps_res_az_true_desired_count_1_df, sps_res_az_false_desired_count_1_df = load_sps.collect_spot_placement_score_first_time(desired_counts=[1])

            sps_res_az_true_desired_count_loop_df, sps_res_az_false_desired_count_loop_df = load_sps.collect_spot_placement_score(
                desired_counts=[desired_count])

            sps_res_specific_az_true_df, sps_res_specific_az_false_df = load_sps.collect_spot_placement_score(
                desired_counts=specific_desired_counts, instance_types=specific_instance_types)


        elif action == EVERY_10MIN_ACTION:
            # UTC 15:00 (KST 00:00)인 경우 실행 건너뛰기
            if event_time_utc_datetime.strftime("%H:%M") == UTC_1500_TIME:
                Logger.info("Skipping scheduled time (UTC 15:00, KST 00:00)")
                return handle_response(200, "Executed successfully. Scheduled time skipped.", action, event_time_utc_datetime)
            sps_res_az_true_desired_count_1_df, sps_res_az_false_desired_count_1_df = load_sps.collect_spot_placement_score(desired_counts=[1])

            sps_res_az_true_desired_count_loop_df, sps_res_az_false_desired_count_loop_df = load_sps.collect_spot_placement_score(
                desired_counts=[desired_count])

            sps_res_specific_az_true_df, sps_res_specific_az_false_df = load_sps.collect_spot_placement_score(
                desired_counts=specific_desired_counts, instance_types=specific_instance_types)

        else:
            raise ValueError(f"Invalid lambda action.")


        if sps_res_az_true_desired_count_1_df is None: raise ValueError("sps_res_az_true_desired_count_1_df is None")
        if sps_res_az_true_desired_count_loop_df is None: raise ValueError("sps_res_az_true_desired_count_loop_df is None")
        if sps_res_az_false_desired_count_loop_df is None: raise ValueError("sps_res_az_false_desired_count_loop_df is None")
        if sps_res_specific_az_true_df is None: raise ValueError("sps_res_specific_az_true_df is None")
        if sps_res_specific_az_false_df is None: raise ValueError("sps_res_specific_az_false_df is None")

        price_saving_if_df = S3.read_file(AZURE_CONST.S3_LATEST_PRICE_SAVING_IF_GZIP_SAVE_PATH, 'pkl.gz')
        if price_saving_if_df is None:
            raise ValueError("price_if_df is None")

        if not handle_res_df_for_spotlake(price_saving_if_df, sps_res_az_true_desired_count_1_df, event_time_utc_datetime):
            raise RuntimeError("Failed to handle_res_df_for_spotlake")

        if not handle_res_df_for_research(sps_res_az_false_desired_count_1_df, sps_res_az_true_desired_count_loop_df, sps_res_az_false_desired_count_loop_df,
                                          sps_res_specific_az_true_df, sps_res_specific_az_false_df,
                                          event_time_utc_datetime):
            raise RuntimeError("Failed to handle_res_for_research_df")

        return handle_response(200, "Executed Successfully!", action, event_time_utc_datetime)

    except Exception as e:
        error_msg = f"Unexpected error: {e}"
        Logger.error(error_msg)
        Logger.error(traceback.format_exc())
        send_slack_message(f"LOCAL TEST AZURE SPS MODULE EXCEPTION!\n{error_msg}\log_stream_id: {log_stream_name}")
        return handle_response(500, "Execute Failed!", action, event_time_utc_datetime, str(e))


def handle_res_df_for_spotlake(price_saving_if_df, sps_res_az_true_desired_count_1_df, time_datetime):
    try:
        time_str = time_datetime.strftime("%Y-%m-%d %H:%M:%S")
        sps_res_az_true_desired_count_1_df['time'] = time_str
        sps_res_az_true_desired_count_1_df['AvailabilityZone'] = sps_res_az_true_desired_count_1_df['AvailabilityZone'].where(pd.notna(sps_res_az_true_desired_count_1_df['AvailabilityZone']), None)

        sps_res_az_true_desired_count_1_merged_df = merge_if_saving_price_sps_df(price_saving_if_df, sps_res_az_true_desired_count_1_df, True)

        prev_availability_zone_true_all_data_df = S3.read_file(
            f"{AZURE_CONST.S3_LATEST_ALL_DATA_AVAILABILITY_ZONE_TRUE_PKL_GZIP_SAVE_PATH}", 'pkl.gz')

        workload_cols = ['InstanceTier', 'InstanceType', 'Region', 'AvailabilityZone', 'DesiredCount']
        feature_cols = ['OndemandPrice', 'SpotPrice', 'IF', 'Score', 'SPS_Update_Time']

        if prev_availability_zone_true_all_data_df is not None and not prev_availability_zone_true_all_data_df.empty:
            prev_availability_zone_true_all_data_df.drop(columns=['id'], inplace=True)
            changed_df = compare_sps(prev_availability_zone_true_all_data_df, sps_res_az_true_desired_count_1_merged_df, workload_cols, feature_cols)
            query_success = query_selector(changed_df)
            timestream_success = upload_timestream(changed_df, time_datetime)
            cloudwatch_success = upload_cloudwatch(sps_res_az_true_desired_count_1_merged_df, time_datetime)

        update_latest_success = update_latest(sps_res_az_true_desired_count_1_merged_df)
        save_raw_az_true_desired_count_1_success = save_raw(sps_res_az_true_desired_count_1_merged_df, time_datetime,
                                data_type='az_true_desired_count_1')

        success_flag = all([query_success, timestream_success, cloudwatch_success, update_latest_success, save_raw_az_true_desired_count_1_success])
        log_details = (
            f"update_latest_success: {update_latest_success}, save: {save_raw_az_true_desired_count_1_success}, cloudwatch: {cloudwatch_success}"
            f"query: {query_success}, timestream: {timestream_success}"
        )

        if success_flag:
            Logger.info("Successfully merged the price/if/sps df, process data for spotlake!")
            return True
        else:
            Logger.info("Failed to merge the price/if/sps df, process data for spotlake!")
            Logger.error(log_details)
            return False

    except Exception as e:
        Logger.error(f"Error in handle_res_df function: {e}")
        return False


def handle_res_df_for_research(sps_res_az_false_desired_count_1_df, sps_res_az_true_desired_count_loop_df, sps_res_az_false_desired_count_loop_df,
                                          sps_res_specific_az_true_df, sps_res_specific_az_false_df, time_datetime):
    try:
        time_str = time_datetime.strftime("%Y-%m-%d %H:%M:%S")
        sps_res_az_false_desired_count_1_df['time'] = time_str
        sps_res_az_true_desired_count_loop_df['time'] = time_str
        sps_res_az_false_desired_count_loop_df['time'] = time_str
        sps_res_specific_az_true_df['time'] = time_str
        sps_res_specific_az_false_df['time'] = time_str

        save_raw_az_false_desired_count_1_success = save_raw(sps_res_az_true_desired_count_loop_df, time_datetime, data_type='az_false_desired_count_1')
        save_raw_az_true_desired_count_loop_success = save_raw(sps_res_az_true_desired_count_loop_df, time_datetime, data_type='az_true_desired_count_loop')
        save_raw_az_false_desired_count_loop_success = save_raw(sps_res_az_false_desired_count_loop_df, time_datetime, data_type='az_false_desired_count_loop')
        save_raw_specific_az_true_success = save_raw(sps_res_specific_az_true_df, time_datetime, data_type='specific_az_true')
        save_raw_specific_az_false_success = save_raw(sps_res_specific_az_false_df, time_datetime, data_type='specific_az_false')


        success_flag = all([save_raw_az_false_desired_count_1_success, save_raw_az_true_desired_count_loop_success, save_raw_az_false_desired_count_loop_success, save_raw_specific_az_true_success, save_raw_specific_az_false_success])
        if success_flag:
            Logger.info("Successfully merged the price/if/sps df, process data for research!")
            return True
        else:
            Logger.info("Failed to merge the price/if/sps df, process data for research!")
            return False

    except Exception as e:
        Logger.error(f"Error in handle_res_df function: {e}")
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
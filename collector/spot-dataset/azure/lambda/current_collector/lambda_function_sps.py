import os
import load_sps
import pandas as pd
import traceback
from datetime import datetime, timezone
from sps_module import sps_shared_resources
from utils.merge_df import merge_if_saving_price_sps_df
from utils.upload_data import update_latest, save_raw, upload_timestream, query_selector, upload_cloudwatch
from utils.compare_data import compare_sps, compare_max_instance
from utils.pub_service import send_slack_message, Logger, S3, AZURE_CONST
from utils.azure_auth import get_sps_token_and_subscriptions

availability_zones = os.environ.get("availability_zones", "False").lower() == "true"
DESIRED_COUNTS = [1, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50]
SPS_METADATA_S3_KEY = f"{AZURE_CONST.S3_RAW_DATA_PATH}/localfile/sps_metadata.yaml"

def read_metadata():
    try:
        data = S3.read_file(SPS_METADATA_S3_KEY, 'yaml')
        if data:
            Logger.info(f"Read metadata from S3: {SPS_METADATA_S3_KEY}")
            return data
    except Exception as e:
        Logger.info(f"Failed to read metadata from S3: {e}")
    
    return None

def write_metadata(metadata):
    try:
        S3.upload_file(metadata, SPS_METADATA_S3_KEY, 'yaml')
        Logger.info(f"Saved metadata to S3: {SPS_METADATA_S3_KEY}")
    except Exception as e:
        Logger.error(f"Failed to save metadata to S3: {e}")

def lambda_handler(event, context):
    log_stream_name = context.log_stream_name
    event_time_utc = event.get("time")
    if event_time_utc:
        try:
            event_time_utc_datetime = datetime.strptime(event_time_utc, "%Y-%m-%dT%H:%M:%SZ")
        except (ValueError, TypeError):
            try:
                event_time_utc_datetime = datetime.strptime(event_time_utc, "%Y-%m-%d %H:%M:%S")
            except:
                 event_time_utc_datetime = datetime.now(timezone.utc)
    else:
        event_time_utc_datetime = datetime.now(timezone.utc)
    
    # Check Token Refresh for Warm Containers
    try:
        sps_shared_resources.sps_token, sps_shared_resources.subscriptions = get_sps_token_and_subscriptions(availability_zones)
        Logger.info("Successfully refreshed Azure SPS Token and Subscriptions.")
    except Exception as e:
        Logger.error(f"Failed to refresh Azure Token: {e}")
        # Proceeding might fail, but letting it try or failing here? 
        # Better to fail early or let existing logic handle it. 
        # Given the error is critical, we log it. logic below might fail.

    sps_shared_resources.succeed_to_get_next_available_location_count_all = 0
    current_date = event_time_utc_datetime.strftime("%Y-%m-%d")
    
    try:
        Logger.info(f"Lambda triggered: event_time: {datetime.strftime(event_time_utc_datetime, '%Y-%m-%d %H:%M:%S')}")

        metadata = read_metadata()
        sps_df = None

        if metadata:
            # --- New Logic: S3 Metadata Exists ---
            # 1. Determine Desired Count (Seamless Rotation)
            desired_count_index = metadata["desired_count_index"]
            current_desired_count = DESIRED_COUNTS[desired_count_index]
            
            # 2. Determine Execution Parameters (Date Check & Index Rotation)
            workload_date = metadata.get("workload_date")
            is_first_time_optimization = False
            
            # Check Workload Date
            if workload_date != current_date:
                Logger.info(f"Workload date changed: {workload_date} -> {current_date}. Prepared First Time Optimization.")
                is_first_time_optimization = True
                
                # Update Metadata: Date
                metadata["workload_date"] = current_date
                
                # Force Desired Count to 1 for First Time Optimization execution
                # Note: We do NOT reset the index here. We continue rotation seamlessly.
                current_execution_desired_count = 1
            else:
                current_execution_desired_count = current_desired_count
            
            # Update Metadata: Next Index (Always rotate to prevent stuck loops)
            next_index = (desired_count_index + 1) % len(DESIRED_COUNTS)
            metadata["desired_count_index"] = next_index
            
            # 3. Save Metadata (State Commit BEFORE Execution)
            try:
                write_metadata(metadata)
            except Exception as e:
                Logger.error(f"Failed to write metadata: {e}")
                # Log but proceed. If write failed, we might retry same index next time,
                # but if execution succeeds, at least data is collected.
                # If execution also fails, we risk loop, but S3 failure is rare compared to API Timeout.

            # 4. Execute Logic
            if is_first_time_optimization:
                Logger.info(f"Executing First Time Optimization with Count: {current_execution_desired_count} (Forced)")
                sps_df = load_sps.collect_spot_placement_score_first_time(desired_counts=[current_execution_desired_count])
            else:
                Logger.info(f"Executing Regular Collection. Desired Count: {current_execution_desired_count} (Index: {desired_count_index})")
                sps_df = load_sps.collect_spot_placement_score(desired_counts=[current_execution_desired_count])


        else:
            # --- Legacy Fallback Logic: S3 Metadata Missing ---
            Logger.info("Metadata missing. Using legacy calculation logic and bootstrapping metadata.")
            time_str_hm = event_time_utc_datetime.strftime("%H:%M")
            UTC_0000_TIME = "00:00"
            
            if time_str_hm == UTC_0000_TIME:
                Logger.info("Legacy Logic: First Time Optimization (00:00)")
                current_desired_count = 1
                sps_df = load_sps.collect_spot_placement_score_first_time(desired_counts=[current_desired_count])
            else:
                # Get desired count from map
                current_desired_count = sps_shared_resources.time_desired_count_map.get(time_str_hm, 1)
                Logger.info(f"Legacy Logic: Regular Collection. Desired Count from Map: {current_desired_count}")
                sps_df = load_sps.collect_spot_placement_score(desired_counts=[current_desired_count])

            # Bootstrap Metadata Immediately
            # Find closest index for current_capacity or default to 0
            try:
                init_index = DESIRED_COUNTS.index(current_desired_count)
            except ValueError:
                init_index = 0
            
            # Prepare next index
            next_index = (init_index + 1) % len(DESIRED_COUNTS)

            new_metadata = {
                "desired_count_index": next_index,
                "workload_date": current_date
            }
            write_metadata(new_metadata)

        if sps_df is None:
             raise ValueError("sps_df is None")

        # Step 3: Handle Results
        if availability_zones is True:
            price_saving_if_df = S3.read_file(AZURE_CONST.S3_LATEST_PRICE_SAVING_IF_GZIP_SAVE_PATH, 'pkl.gz')
            if price_saving_if_df is None:
                raise ValueError("price_if_df is None")

            if not handle_res_df_for_spotlake(price_saving_if_df, sps_df, event_time_utc_datetime, current_desired_count):
                raise RuntimeError("Failed to handle_res_df_for_spotlake")

        if not handle_res_df_for_research(sps_df, event_time_utc_datetime, current_desired_count):
            raise RuntimeError("Failed to handle_res_for_research_df")

        print(f"succeed_to_get_next_available_location_count_all: {sps_shared_resources.succeed_to_get_next_available_location_count_all}")

        analyze_id_location_data()
        return handle_response(200, "Executed Successfully!", event_time_utc_datetime)

    except Exception as e:
        error_msg = f"Unexpected error: {e}"
        Logger.error(error_msg)
        Logger.error(traceback.format_exc())
        send_slack_message(f"AZURE SPS MODULE EXCEPTION!\n{error_msg} \nlog_stream_id: {log_stream_name}")
        return handle_response(500, "Execute Failed!", event_time_utc_datetime, str(e))


def handle_res_df_for_spotlake(price_saving_if_df, sps_df, time_datetime, desired_count):
    try:
        time_str = time_datetime.strftime("%Y-%m-%d %H:%M:%S")
        sps_df['time'] = time_str
        sps_df['AvailabilityZone'] = sps_df['AvailabilityZone'].where(pd.notna(sps_df['AvailabilityZone']), None)

        sps_merged_df = merge_if_saving_price_sps_df(price_saving_if_df, sps_df, True)

        prev_availability_zone_true_all_data_df = S3.read_file(
            f"{AZURE_CONST.S3_LATEST_ALL_DATA_AVAILABILITY_ZONE_TRUE_PKL_GZIP_SAVE_PATH}", 'pkl.gz')

        workload_cols = ['InstanceTier', 'InstanceType', 'Region', 'AvailabilityZone', 'DesiredCount']
        feature_cols = ['OndemandPrice', 'SpotPrice', 'IF', 'Score', 'SPS_Update_Time', 'T2', 'T3']

        query_success = timestream_success = cloudwatch_success = \
            update_latest_success = save_raw_success = False

        if prev_availability_zone_true_all_data_df is not None and not prev_availability_zone_true_all_data_df.empty:
            prev_availability_zone_true_all_data_df.drop(columns=['id'], inplace=True, errors='ignore')
            
            # Apply T2/T3 Aggregation Logic
            sps_merged_df = compare_max_instance(prev_availability_zone_true_all_data_df, sps_merged_df, desired_count)
            
            changed_df = compare_sps(prev_availability_zone_true_all_data_df, sps_merged_df, workload_cols, feature_cols)
            
            query_success = query_selector(changed_df)
            timestream_success = upload_timestream(changed_df, time_datetime)
            cloudwatch_success = upload_cloudwatch(sps_merged_df, time_datetime)

        update_latest_success = update_latest(sps_merged_df)
        
        data_type = 'desired_count_1' if desired_count == 1 else 'multi'
        
        save_raw_success = save_raw(sps_merged_df, time_datetime, availability_zones, data_type=data_type)

        success_flag = all([query_success, timestream_success, cloudwatch_success, update_latest_success, save_raw_success])
        log_details = (
            f"update_latest_success: {update_latest_success}, save: {save_raw_success}, cloudwatch: {cloudwatch_success}"
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
        Logger.error(f"Error in handle_res_df_for_spotlake function: {e}")
        return False


def handle_res_df_for_research(sps_df, time_datetime, desired_count):
    try:
        time_str = time_datetime.strftime("%Y-%m-%d %H:%M:%S")
        sps_df['time'] = time_str
        
        data_type = 'desired_count_1' if desired_count == 1 else 'multi'

        if availability_zones is True:
            pass # Previously handled
        else:
            # AZ=False
            save_raw_success = save_raw(sps_df, time_datetime, availability_zones, data_type=data_type)
            return save_raw_success
        
        return True

    except Exception as e:
        Logger.error(f"Error in handle_res_df_for_research function: {e}")
        return False


def analyze_id_location_data():
    id_count = len(sps_shared_resources.locations_call_history_tmp)
    if id_count > 0:
        first_id = list(sps_shared_resources.locations_call_history_tmp.keys())[0]
        location_count = len(sps_shared_resources.locations_call_history_tmp[first_id])
    else:
        location_count = 0

    max_time_occurrence = id_count * location_count * 10
    total_time_occurrences = 0
    for id_data in sps_shared_resources.locations_call_history_tmp.values():
        for location_data in id_data.values():
            total_time_occurrences += len(location_data)

    print(f"\n-----------------------")
    print(f"1. 구독 ID의 개수: {id_count}, 단일 구독 Location 개수: {location_count}")
    print(f"2. 한 시간에 최대 호출 가능 횟수: {max_time_occurrence}")
    print(f"3. 한 시간에 이미 호출: {total_time_occurrences}, 남은 가능 호출 수: {max_time_occurrence - total_time_occurrences}")
    print(f"-----------------------")


def handle_response(status_code, body, time_datetime, error_message=None):
    response = {
        "statusCode": status_code,
        "body": body,
        "time": datetime.strftime(time_datetime, '%Y-%m-%d %H:%M:%S')
    }
    if error_message:
        response["error_message"] = error_message

    Logger.info(f"Response: {response}")
    return response
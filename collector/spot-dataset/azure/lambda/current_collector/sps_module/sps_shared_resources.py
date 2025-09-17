from threading import RLock
from datetime import datetime, timedelta

bad_request_retry_count = 0
found_invalid_instance_type_retry_count = 0
found_invalid_region_retry_count = 0
time_out_retry_count = 0
succeed_to_get_sps_count = 0
succeed_to_get_next_available_location_count = 0
too_many_requests_count = 0
too_many_requests_count_2 = 0
succeed_to_get_next_available_location_count_all = 0
lock = RLock()
location_lock = RLock()

sps_token = None
invalid_regions_tmp = None
invalid_instance_types_tmp = None
locations_call_history_tmp = None
locations_over_limit_tmp = None
last_subscription_id_and_location = None
region_map_and_instance_map_tmp = None
subscriptions = None
available_locations = None


def generate_time_to_desired_count_map():
    start_time = datetime.strptime("15:00", "%H:%M")
    end_time = datetime.strptime("14:50", "%H:%M")
    desired_counts = [5, 10, 15, 20, 25, 30, 35, 40, 45, 50]
    time_to_desired_count = {}
    current_time = start_time
    index = 0
    while True:
        time_str = current_time.strftime("%H:%M")
        time_to_desired_count[time_str] = desired_counts[index % len(desired_counts)]
        if current_time.hour == end_time.hour and current_time.minute == end_time.minute:
            break
        current_time += timedelta(minutes=10)
        index += 1
    return time_to_desired_count

time_desired_count_map = generate_time_to_desired_count_map()
from threading import RLock

bad_request_retry_count = 0
found_invalid_instance_type_retry_count = 0
found_invalid_region_retry_count = 0
time_out_retry_count = 0
too_many_requests_count = 0
lock = RLock()
location_lock = RLock()

sps_token = None
invalid_regions_tmp = None
invalid_instance_types_tmp = None
locations_call_history_tmp = None
locations_over_limit_tmp = None
last_subscription_id_and_location_tmp = None
subscriptions = None
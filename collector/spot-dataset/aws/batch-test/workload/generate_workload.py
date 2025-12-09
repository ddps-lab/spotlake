# reference
# https://developers.google.com/optimization/bin/bin_packing

# ------ import module ------
import boto3
import botocore
import pickle
import gzip
from datetime import datetime, timezone, timedelta
from ortools.linear_solver import pywraplp
import io
import argparse

# ------ import user module ------
from utility.slack_msg_sender import send_slack_message
from load_metadata import num_az_by_region


# create object of bin packing input data
def create_data_model(weights, capacity):
    data = {}
    data['weights'] = weights
    data['items'] = list(range(len(weights)))
    data['bins'] = data['items']
    data['bin_capacity'] = capacity
    return data


# run bin packing with algorithm name
def bin_packing(weights, capacity, algorithm):
    bin_index_list = []
    data = create_data_model(weights, capacity)
    solver = pywraplp.Solver.CreateSolver(algorithm)

    x = {}
    for i in data['items']:
        for j in data['bins']:
            x[(i, j)] = solver.IntVar(0, 1, 'x_%i_%i' % (i, j))

    y = {}
    for j in data['bins']:
        y[j] = solver.IntVar(0, 1, 'y[%i]' % j)

    for i in data['items']:
        solver.Add(sum(x[i, j] for j in data['bins']) == 1)

    for j in data['bins']:
        solver.Add(
            sum(x[(i, j)] * data['weights'][i] for i in data['items']) <= y[j] *
            data['bin_capacity'])

    solver.Minimize(solver.Sum([y[j] for j in data['bins']]))
    status = solver.Solve()
    if status == pywraplp.Solver.OPTIMAL:
        for j in data['bins']:
            if y[j].solution_value() == 1:
                bin_items = []
                bin_weight = 0
                for i in data['items']:
                    if x[i, j].solution_value() > 0:
                        bin_items.append(i)
                        bin_weight += data['weights'][i]
                if bin_weight > 0:
                    bin_index_list.append((bin_items, bin_weight))
        return bin_index_list
    else:
        send_slack_message("The problem does not have an optimal solution.")
        print('The problem does not have an optimal solution.')


# run bin packing algorithm to instance-region workloads
def workload_bin_packing(query, capacity, algorithm):
    weights = [weight for instance, weight in query]
    bin_index_list = bin_packing(weights, 10, algorithm)
    
    binpacked = []
    
    for bin_index, bin_weight in bin_index_list:
        binpacked.append([(query[x][0], query[x][1]) for x in bin_index])
    
    return binpacked


def get_binpacked_workload(filedate):
    s3_client = boto3.client('s3')
    s3_resource = boto3.resource('s3')
    
    print("Collecting metadata (num_az_by_region)...")
    workloads = num_az_by_region()

    start_time = datetime.now(timezone.utc)
    # Upload raw workloads to monitoring path
    # Original: monitoring/{filedate}/workloads.pkl
    # We should use s3_path_prefix if possible, but original code hardcoded 'monitoring'.
    # Let's stick to original path structure relative to bucket root if not specified otherwise,
    # or use S3_PATH_PREFIX/monitoring?
    # User request says "collector/spot-dataset/aws/batch/".
    # The original code used os.environ.get('S3_BUCKET') and hardcoded paths.
    # I will use S3_PATH_PREFIX if set, else root.
    
    S3_PATH_PREFIX = "rawdata/aws"
    BUCKET_NAME = "spotlake-test"
    
    monitoring_key = f"{S3_PATH_PREFIX}/monitoring/{filedate}/workloads.pkl"
         
    print(f"Uploading raw workloads to {monitoring_key}...")
    s3_resource.Object(BUCKET_NAME, monitoring_key).put(Body=pickle.dumps(workloads))
    
    end_time = datetime.now(timezone.utc)
    print(f"Upload time used for minitoring is {(end_time - start_time).total_seconds() * 1000 / 60000:.2f} min")

    print("Starting bin packing...")
    result_binpacked = {}
    
    for instance, query in workloads.items():
        result_binpacked[instance] = workload_bin_packing(query, 10, 'CBC')
    
    user_queries_list = []
    user_queries = []
    for instance, queries in result_binpacked.items():
        for query in queries:
            new_query = [instance, [], 0]
            for tup in query:
                new_query[1].append(tup[0])
                new_query[2] += tup[1]
            user_queries.append(new_query)
            if len(user_queries) == 50:
                user_queries_list.append(user_queries)
                user_queries = []

    if len(user_queries) != 0:
        user_queries_list.append(user_queries)
    
    start_time = datetime.now(timezone.utc)
    try:
        buffer = io.BytesIO()
        pickle.dump(user_queries_list, buffer)
        buffer.seek(0)

        compressed_buffer = io.BytesIO()
        with gzip.GzipFile(fileobj=compressed_buffer, mode='wb') as gz:
            gz.write(buffer.getvalue())
        compressed_buffer.seek(0)
    except Exception as e:
        send_slack_message(e)
        raise e

    try:
        # Original: {os.environ.get('PARENT_PATH')}/workloads/{filedate}/binpacked_workloads.pkl.gz
        # PARENT_PATH was likely rawdata/aws
        # Original: {os.environ.get('PARENT_PATH')}/workloads/{filedate}/binpacked_workloads.pkl.gz
        # PARENT_PATH was likely rawdata/aws
        S3_PATH_PREFIX = "rawdata/aws"
        BUCKET_NAME = "spotlake-test"
        workload_key = f"{S3_PATH_PREFIX}/workloads/{filedate}/binpacked_workloads.pkl.gz"
        print(f"Uploading binpacked workloads to {workload_key}...")
        s3_client.upload_fileobj(compressed_buffer, BUCKET_NAME, workload_key)
    except Exception as e:
        send_slack_message(e)
        raise e
        
    end_time = datetime.now(timezone.utc)
    print(f"Upload time used for collecting is {(end_time - start_time).total_seconds() * 1000 / 60000:.2f} min")

    return user_queries_list


def main():
    # ------ Parse Arguments ------
    parser = argparse.ArgumentParser()
    parser.add_argument('--timestamp', dest='timestamp', action='store', help='Timestamp in format YYYY-MM-DDTHH:MM')
    args = parser.parse_args()

    # ------ Set time data ------
    start_time = datetime.now(timezone.utc)
    
    if args.timestamp:
        # Handle EventBridge timestamp format (YYYY-MM-DDTHH:MM:SSZ)
        if args.timestamp.endswith('Z'):
            timestamp = datetime.strptime(args.timestamp, "%Y-%m-%dT%H:%M:%SZ")
        else:
            try:
                timestamp = datetime.strptime(args.timestamp, "%Y-%m-%dT%H:%M:%S")
            except ValueError:
                timestamp = datetime.strptime(args.timestamp, "%Y-%m-%dT%H:%M")
    else:
        # Default to next day as per original logic
        timestamp = start_time.replace(minute=((start_time.minute // 10) * 10), second=0) + timedelta(days=1)
        
    S3_DIR_NAME = timestamp.strftime('%Y/%m/%d')
    print(f"Generating workload for date: {S3_DIR_NAME}")
    
    # ------ Collect Spot Price ------
    try:
        workload = get_binpacked_workload(S3_DIR_NAME)
    except botocore.exceptions.ClientError as e:
        send_slack_message(e)
        print(e)
        raise e
        
    end_time = datetime.now(timezone.utc)
    print(f"collecting time is {(end_time - start_time).total_seconds() * 1000 / 60000:.2f} min")

    # ------ Save Raw Data in S3 ------
    s3_client = boto3.client('s3')
    try:
        saving_start_time = datetime.now(timezone.utc)
        buffer = io.BytesIO()
        pickle.dump(workload, buffer)
        buffer.seek(0)
        
        S3_PATH_PREFIX = "rawdata/aws"
        BUCKET_NAME = "spotlake-test"
        localfile_key = f"{S3_PATH_PREFIX}/localfile/workloads.pkl"
        print(f"Uploading backup to {localfile_key}...")
        s3_client.upload_fileobj(buffer, BUCKET_NAME, localfile_key)
    except Exception as e:
        send_slack_message(e)
        print(e)
        raise e
        
    end_time = datetime.now(timezone.utc)
    print(f"Upload time used for back-up is {(end_time - saving_start_time).total_seconds() * 1000 / 60000:.2f} min")
    print(f"Running time is {(end_time - start_time).total_seconds() * 1000 / 60000:.2f} min")

if __name__ == "__main__":
    main()

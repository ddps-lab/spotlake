# reference
# https://developers.google.com/optimization/bin/bin_packing

import boto3
import botocore
import pickle
import sys
import os
import gzip
import argparse
from datetime import datetime
from ortools.linear_solver import pywraplp
from load_metadata import num_az_by_region

sys.path.append('/home/ubuntu/spotlake/utility')

from slack_msg_sender import send_slack_message

sys.path.append('/home/ubuntu/spotlake')

from const_config import AwsCollector, Storage

STORAGE_CONST = Storage()
AWS_CONST = AwsCollector()

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
    # reverse order of credential data
    user_cred = None
    try:
        user_cred = pickle.load(open(f"{AWS_CONST.LOCAL_PATH}/user_cred_df.pkl", "rb"))
    except:
        user_cred = pickle.load(s3.Object(STORAGE_CONST.BUCKET_NAME, f'{AWS_CONST.S3_LOCAL_FILES_SAVE_PATH}/user_cred_df.pkl').get()['Body'])
    user_cred = user_cred[::-1]
    pickle.dump(user_cred, open(f"{AWS_CONST.LOCAL_PATH}/user_cred_df.pkl", "wb"))
    with open(f"{AWS_CONST.LOCAL_PATH}/user_cred_df.pkl", 'rb') as f:
        s3.upload_fileobj(f, STORAGE_CONST.BUCKET_NAME, f'{AWS_CONST.S3_LOCAL_FILES_SAVE_PATH}/user_cred_df.pkl')

    DIRLIST = os.listdir(f"{AWS_CONST.LOCAL_PATH}/")
    if f"{filedate}_binpacked_workloads.pkl" in DIRLIST:
        binpacked_workload = pickle.load(open(f"{AWS_CONST.LOCAL_PATH}/{filedate}_binpacked_workloads.pkl", 'rb'))
        return binpacked_workload
    else:
        for filename in DIRLIST:
            if "binpacked_workloads.pkl" in filename:
                os.remove(f"{AWS_CONST.LOCAL_PATH}/{filename}")
    
    s3_resource = boto3.resource('s3')

    workloads = num_az_by_region()
    today = "/".join(filedate.split("-"))
    # need to change file location
    s3_resource.Object(STORAGE_CONST.BUCKET_NAME, f"monitoring/{today}/workloads.pkl").put(Body=pickle.dumps(workloads))

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
        user_queries = []    

    # need to change file location
    s3_client = boto3.client('s3')
    pickle.dump(user_queries_list, open(f"{AWS_CONST.LOCAL_PATH}/{filedate}_binpacked_workloads.pkl", 'wb'))
    gzip.open(f"{AWS_CONST.LOCAL_PATH}/{filedate}_binpacked_workloads.pkl.gz", "wb").writelines(open(f"{AWS_CONST.LOCAL_PATH}/{filedate}_binpacked_workloads.pkl", "rb"))
    s3_client.upload_fileobj(open(f"{AWS_CONST.LOCAL_PATH}/{filedate}_binpacked_workloads.pkl.gz", "rb"), STORAGE_CONST.BUCKET_NAME, f"rawdata/aws/workloads/{today}/binpacked_workloads.pkl.gz")

    return user_queries_list

if __name__=="__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--timestamp', dest='timestamp', action='store')
    args = parser.parse_args()
    timestamp = datetime.strptime(args.timestamp, "%Y-%m-%dT%H:%M")
    date = args.timestamp.split("T")[0]
    
    try:
        workload = get_binpacked_workload(date)
    except botocore.exceptions.ClientError as e:
        send_slack_message(e)
    s3_client = boto3.client('s3')
    pickle.dump(workload, open(f"{AWS_CONST.LOCAL_PATH}/workloads.pkl", "wb"))
    s3_client.upload_fileobj(open(f"{AWS_CONST.LOCAL_PATH}/workloads.pkl", "rb"), STORAGE_CONST.BUCKET_NAME, f"{AWS_CONST.S3_LOCAL_FILES_SAVE_PATH}/workloads.pkl")


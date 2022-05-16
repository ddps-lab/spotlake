import pickle
from ortools.linear_solver import pywraplp
import time
from main_num_az import collect_num_az
import urllib.request, urllib.parse, json

def create_data_model(weights, capacity):
    """Create the data for the example."""
    data = {}
    data['weights'] = weights
    data['items'] = list(range(len(weights)))
    data['bins'] = data['items']
    data['bin_capacity'] = capacity
    return data

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
        print('The problem does not have an optimal solution.')

def CBC(query, capacity):
    weights = [weight for instance, weight in query]
    bin_index_list = bin_packing(weights, 10, 'CBC')
    
    to_return = []
    
    for bin_index, bin_weight in bin_index_list:
        to_return.append([(query[x][0], query[x][1]) for x in bin_index])
    
    return to_return

def generate_curl_message(message):
    payload = {"text": message}
    return json.dumps(payload).encode("utf-8")

def post_message(url, data):
    req = urllib.request.Request(url)
    req.add_header("Content-Type", "application/json")
    return urllib.request.urlopen(req, data)

if __name__ == "__main__":
    
    collect_num_az()

    region_cnt = pickle.load(open('/home/ec2-user/WorkloadCreator/base.pickle', 'rb'))

    result_binpacked = {}
    nums = 0
    
    start_time = time.time()
    
    for instance, query in region_cnt.items():
        result_binpacked[instance] = CBC(query, 10)
        nums += len(result_binpacked[instance])
    
    workload_result = []
    f_query = []
    for instance, queries in result_binpacked.items():
        for query in queries:
            new_query = [instance, [], 0]
            for tup in query:
                new_query[1].append(tup[0])
                new_query[2] += tup[1]
            f_query.append(new_query)
            if len(f_query) == 50:
                workload_result.append(f_query)
                f_query = []

    end_time = time.time()
    
    print(end_time - start_time)
    
    pickle.dump(workload_result, open('/home/ec2-user/SpotInfo/pkls/bin_packed_workload.pickle', 'wb'))
    print(nums)

    user_cred = pickle.load(open('/home/ec2-user/SpotInfo/pkls/user_cred_df.pkl', 'rb'))
    user_cred = user_cred[::-1]
    pickle.dump(user_cred, open('/home/ec2-user/SpotInfo/pkls/user_cred_df.pkl', 'wb'))

    url = "https://hooks.slack.com/services/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"
    message = f"Spot Placement Score Monitoring - the number of necessary accounts to fetch all scores {len(workload_result)}"
    data = generate_curl_message(message)
    response = post_message(url, data)
    print(response.status)

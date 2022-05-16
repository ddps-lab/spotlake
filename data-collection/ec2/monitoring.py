import urllib.request, urllib.parse, json
import pickle

def generate_curl_message(message):
    payload = {"text": message}
    return json.dumps(payload).encode("utf-8")

def post_message(url, data):
    req = urllib.request.Request(url)
    req.add_header("Content-Type", "application/json")
    return urllib.request.urlopen(req, data)

if __name__ == "__main__":
    workload_result = pickle.load(open("/home/ec2-user/SpotInfo/pkls/bin_packed_workload.pickle", "rb"))
    url = "https://hooks.slack.com/services/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"
    message = f"Spot Placement Score Monitoring - the number of necessary accounts to fetch all scores {len(workload_result)}"
    data = generate_curl_message(message)
    response = post_message(url, data)
    print(response.status)

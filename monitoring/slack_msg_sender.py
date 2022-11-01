import requests

url = "https://hooks.slack.com/services/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"


def send_msg(msg):

    slack_data = {
        "text": msg
    }
    requests.post(url, json=slack_data)

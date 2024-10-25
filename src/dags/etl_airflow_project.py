import requests
import json

headers = {
    'X-Nickname': 'evmenenko.de',
    'X-Cohort': '30',
    'X-Project': 'True',
    'X-API-KEY': '5f55e6c0-e9e5-4a9c-b313-63c01fc31460',
    'Content-Type': 'application/x-www-form-urlencoded'
}

def create_files_request(ti, api_endpoint, headers):
    method_url = '/generate_report'
    r = requests.post('https://' + api_endpoint + method_url, headers=headers)

    if r.status_code == 200:
        response_dict = json.loads(r.content)
        task_id = response_dict.get('task_id', None)

        if task_id:
            ti.xcom_push(key='task_id', value=task_id)
            print(f"task_id is {task_id}")
            return task_id
        else:
            print("task_id not found in the response.")
            return None
    else:
        print(f"Request failed with status code: {r.status_code}, response: {r.content}")
        return None
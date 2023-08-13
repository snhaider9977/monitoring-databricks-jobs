import requests
import pandas as pd
import math
import datetime
import json

def fetch_and_process_job_runs(base_uri, api_token, params):
    endpoint = '/api/2.1/jobs/runs/list'
    headers = {'Authorization': f'Bearer {api_token}'}
    
    all_data = []  # To store all the data from multiple pages
    
    while True:

        response = requests.get(base_uri + endpoint, headers=headers, params=params)
        response_json = response.json()
       
        
        data = []
        for run in response_json["runs"]:
            start_time_ms = run["start_time"]
            start_time_seconds = start_time_ms / 1000
            start_time_readable = datetime.datetime.fromtimestamp(start_time_seconds).strftime('%Y-%m-%d %H:%M:%S')
            data.append({
                "job_id": run["job_id"],
                "creator_user_name": run["creator_user_name"],
                "run_name": run["run_name"],
                "run_page_url": run["run_page_url"],
                "run_id": run["run_id"],
                "execution_duration_in_mins": math.ceil(int(run.get('execution_duration')) / (1000 * 60)),
                "result_state": run["state"].get("result_state"),
                "start_time": start_time_readable
            })
        
        all_data.extend(data)
        df = pd.DataFrame(all_data)
        print(df)
        
        if response_json.get("has_more") == True:
            next_page_token = response_json.get("next_page_token")
            params['page_token'] = next_page_token
        else:
            break
    
    df = pd.DataFrame(all_data)
    return df

# Replace with your actual values
now = datetime.datetime.utcnow()
yesterday = now - datetime.timedelta(days=1)
start_time_from = int(yesterday.replace(hour=0, minute=0, second=0, microsecond=0).timestamp()) * 1000
start_time_to = int(yesterday.replace(hour=23, minute=59, second=59, microsecond=999999).timestamp()) * 1000
        
params = {
     "start_time_from": start_time_from,
     "start_time_to": start_time_to,
     "expand_tasks": True
}
baseURI = 'https://adbxxxxxxxxxxxxxxx.azuredatabricks.net'
apiToken = 'xxxxxxxxxxxxxxxxxxxxxxxxxxx'

result_df = fetch_and_process_job_runs(baseURI, apiToken, params)
sorted_df = result_df.sort_values(by='execution_duration_in_mins', ascending=False)
total_time = sorted_df['execution_duration_in_mins'].sum()
total_time_row = pd.DataFrame({'job_id': ['Total Time'], 'execution_duration_in_mins': [total_time]})
sorted_df = pd.concat([sorted_df, total_time_row], ignore_index=True)
print(sorted_df)
csv_filename = 'jobs.csv'
sorted_df.to_csv(csv_filename, index=False, float_format='%.0f')




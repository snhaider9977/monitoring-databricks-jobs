import requests
import pandas as pd
import math
import datetime
import json
import matplotlib.pyplot as plt

def fetch_and_process_job_runs(base_uri, api_token, params):
    endpoint = '/api/2.1/jobs/runs/list'
    headers = {'Authorization': f'Bearer {api_token}'}
    
    all_data = []  
    
    while True:
        response = requests.get(base_uri + endpoint, headers=headers, params=params)
        response_json = response.json()
        
        data = []
        for run in response_json["runs"]:
            start_time_ms = run["start_time"]
            start_time_seconds = start_time_ms / 1000
            start_time_readable = datetime.datetime.fromtimestamp(start_time_seconds).strftime('%Y-%m-%d %H:%M:%S')
            job_data = {
                "job_id": run["job_id"],
                "creator_user_name": run["creator_user_name"],
                "run_name": run["run_name"],
                "run_page_url": run["run_page_url"],
                "run_id": run["run_id"],
                "execution_duration_in_mins": math.ceil(int(run.get('execution_duration')) / (1000 * 60)),
                "result_state": run["state"].get("result_state"),
                "start_time": start_time_readable,
                "life_cycle_state": run["state"].get("life_cycle_state"),
                "queue_reason": run["state"].get("queue_reason")
            }
            
            for task in run.get("tasks", []):
                task_data = {
                    "task_key": task.get("task_key"),
                    "depends_on": [dep.get("task_key") for dep in task.get("depends_on", [])],
                    "life_cycle_state": task["state"].get("life_cycle_state"),
                    "queue_reason": task["state"].get("queue_reason")
                }
                data.append({**job_data, **task_data})
        
        all_data.extend(data)
        
        if response_json.get("has_more") == True:
            next_page_token = response_json.get("next_page_token")
            params['page_token'] = next_page_token
        else:
            break
    
    df = pd.DataFrame(all_data)
    return df


now = datetime.datetime.utcnow()
yesterday = now - datetime.timedelta(days=1)
start_time_from = int(yesterday.replace(hour=0, minute=0, second=0, microsecond=0).timestamp()) * 1000
start_time_to = int(yesterday.replace(hour=23, minute=59, second=59, microsecond=999999).timestamp()) * 1000
        
params = {
     "start_time_from": start_time_from,
     "start_time_to": start_time_to,
     "expand_tasks": "true"
}
baseURI = 'https://xxxxxxxxxxxxxxxxxxx.azuredatabricks.net'
apiToken = 'xxxxxxxxxxxxxxxxxxxx'

result_df = fetch_and_process_job_runs(baseURI, apiToken, params)
sorted_df = result_df.sort_values(by='execution_duration_in_mins', ascending=False)
total_time = sorted_df['execution_duration_in_mins'].sum()
total_time_row = pd.DataFrame({'job_id': ['Total Time'], 'execution_duration_in_mins': [total_time]})
sorted_df = pd.concat([sorted_df, total_time_row], ignore_index=True)


sorted_df['task_result'] = sorted_df['result_state'].apply(lambda x: 'Succeeded' if x == 'SUCCESS' else 'Failed')


total_jobs = sorted_df['job_id'].nunique()
total_tasks = len(sorted_df)
successful_tasks = len(sorted_df[sorted_df['result_state'] == 'SUCCESS'])
failed_tasks = len(sorted_df[sorted_df['result_state'] == 'FAILED'])
avg_execution_time = sorted_df['execution_duration_in_mins'].mean()
min_execution_time = sorted_df['execution_duration_in_mins'].min()
max_execution_time = sorted_df['execution_duration_in_mins'].max()
most_failed_jobs = sorted_df[sorted_df['result_state'] == 'FAILED'][['job_id', 'result_state']].head()


report = f"""
KPI Report for {now.strftime('%Y-%m-%d')}:

Total Jobs: {total_jobs}
Total Tasks: {total_tasks}
Successful Tasks: {successful_tasks}
Failed Tasks: {failed_tasks}
Total Execution Time (mins): {total_time}
Average Execution Time (mins): {avg_execution_time:.2f}
Min Execution Time (mins): {min_execution_time}
Max Execution Time (mins): {max_execution_time}

Key Insights:
1. Task Status Distribution:
{json.dumps(sorted_df['result_state'].value_counts().to_dict(), indent=4)}

2. Execution Duration Distribution:
Min: {min_execution_time} mins
Max: {max_execution_time} mins
Average: {avg_execution_time:.2f} mins

3. Jobs with Longest Execution Time:
{sorted_df[['job_id', 'execution_duration_in_mins']].head()}

4. Jobs with Most Failed Tasks:
{most_failed_jobs}



"""

print(report)

# Create visualizations (e.g., bar chart for task status)
plt.figure(figsize=(16, 8))
plt.subplot(2, 2, 1)
task_status_counts = sorted_df['result_state'].value_counts()
plt.bar(task_status_counts.index, task_status_counts.values)
plt.title('Task Status')
plt.xlabel('Status')
plt.ylabel('Count')





plt.tight_layout()


csv_filename = 'jobs.csv'
sorted_df.to_csv(csv_filename, index=False, float_format='%.0f')


plt.show()

<h1>Databricks Job Runs Data monitoring Tool</h1>

<p>This script is designed to fetch and process job runs data from an Azure Databricks instance using the Databricks REST API. It extracts relevant information about job runs, processes the data, and provides an output in the form of a Pandas DataFrame and a CSV file.</p>

<h2>Prerequisites</h2>

<p>Before running this script, ensure you have the following:</p>

<ul>
  <li><strong>Azure Databricks Instance:</strong> You need access to an Azure Databricks instance.</li>
  <li><strong>API Token:</strong> Generate an API token from your Databricks instance with appropriate permissions to access job run data.</li>
</ul>

<h2>Getting Started</h2>

<ol>
  <li>Install the required libraries using the following command:</li>
</ol>

<pre><code>pip install requests pandas</code></pre>

<ol start="2">
  <li>Replace the placeholders in the code with your actual values:</li>
</ol>

<pre><code>baseURI: Replace with your Azure Databricks instance URL.<br>
apiToken: Replace with your API token.</code></pre>

<h2>How the Script Works</h2>

<p>The script starts by importing necessary libraries: requests, pandas, math, datetime, and json.</p>

<h3>Function <code>fetch_and_process_job_runs</code></h3>

<p>The script defines the function <code>fetch_and_process_job_runs</code> responsible for fetching job run data using the Databricks API. The function takes three arguments:</p>

<ul>
  <li><code>base_uri:</code> The base URL of your Databricks instance.</li>
  <li><code>api_token:</code> Your API token for authentication.</li>
  <li><code>params:</code> A dictionary containing query parameters, including start_time_from, start_time_to, and expand_tasks.</li>
</ul>

<p>Inside the function:</p>

<ul>
  <li>An API request is made to the specified endpoint.</li>
  <li>The response is processed to extract job run details.</li>
  <li>Processed data is accumulated and transformed into a Pandas DataFrame.</li>
  <li>Pagination is managed using the <code>has_more</code> field in the response.</li>
</ul>

<h3>Data Analysis and Output</h3>

<p>After fetching and processing the job run data:</p>

<ul>
  <li>The resulting DataFrame is sorted based on the <code>execution_duration_in_mins</code> column in descending order.</li>
  <li>The total execution time for all job runs is calculated and added as a row in the DataFrame.</li>
  <li>The processed DataFrame is saved as a CSV file named <code>jobs.csv</code>.</li>
  <li>The sorted DataFrame is printed to the console.</li>
</ul>

<h2>Usage</h2>

<p>Make sure you have fulfilled the prerequisites and replaced the placeholder values in the code.</p>

<p>Run the script. It will fetch and process job runs data, display the sorted results, save them to a CSV file, and print a Markdown table.</p>

<p><strong>Note:</strong> This script provides a basic example of how to fetch and process job runs data from Azure Databricks using the Databricks REST API. You can further enhance and customize the script to suit your specific use case and requirements.</p>

##output

# KPI Report for 2023-10-07:

Total Jobs: 160
Total Tasks: 214
Successful Tasks: 174
Failed Tasks: 15
Total Execution Time (mins): 1158
Average Execution Time (mins): 10.82
Min Execution Time (mins): 0
Max Execution Time (mins): 1158

**Key Insights:**

1. **Task Status Distribution:**
```json
{
    "SUCCESS": 174,
    "CANCELED": 24,
    "FAILED": 15
}
```
2. **Execution Duration Distribution:**
   - Min: 0 mins
   - Max: 1158 mins
   - Average: 10.82 mins

3. **Jobs with Longest Execution Time:**
   | job_id          | execution_duration_in_mins |
   |-----------------|-----------------------------|
   | 260792223809789 | 140                         |
   | 74519312719017  | 93                          |
   | 371241484431340 | 88                          |
   | 655421446142082 | 85                          |
   | 887636488212750 | 65                          |


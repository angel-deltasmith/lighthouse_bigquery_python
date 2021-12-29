#BigQuery Connection
#!pip install --upgrade google-cloud-BigQuery
#!export GOOGLE_APPLICATION_CREDENTIALS="/content/dbtkey.json"
#!pip3 install --upgrade google-cloud-bigquery
#!pip install --upgrade google-cloud
#!pip install --upgrade google-cloud-bigquery
#!pip install --upgrade google-cloud-bigquery
#pip install pyarrow
from google.cloud.bigquery.client import Client
from google.cloud import bigquery
import os

#Credentials
credential_path  ="C:\\Users\\mlope\\OneDrive\\Documentos\\Lighthouse\\Lighthouse_working\\dbtkey.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path

#Client
client = Client.from_service_account_json(credential_path, project='dbt-project-335000')

#Output_Table
table_id_output = 'dbt-project-335000.dbt_mangel.output2'

#Setup for writting in the table
job_config = bigquery.QueryJobConfig(destination=table_id_output)#parameters = destination table to write 
job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND #If the table already exists, then append the rows

#Select URLs from input table
df = client.query('''
  SELECT 
    * 
  FROM `dbt-project-335000.dbt_mangel.input_test`''').result().to_dataframe()

#print(df.head())

for url in df['url']:
        print('INFO URLs:'+url) 






















#Get the values from the input table
sql = """
    SELECT 
    * 
  FROM `dbt-project-335000.dbt_mangel.input_test`;
"""

# Start the query, passing in the extra configuration.
#query_job = client.query(sql, job_config=job_config)  # Make an API request.
#query_job.result()  # Wait for the job to complete.


print("Query results loaded to the table {}".format(table_id_output))

datasets = list(client.list_datasets())  # Make an API request.
project = client.project

if datasets:
    print("Datasets in project {}:".format(project))
    for dataset in datasets:
        print("\t{}".format(dataset.dataset_id))
else:
    print("{} project does not contain any datasets.".format(project))

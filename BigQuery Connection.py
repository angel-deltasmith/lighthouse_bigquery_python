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
import json
import os
import pandas as pd
import time
from datetime import datetime
from os.path import join

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

#Variables
#df = pd.DataFrame([], columns=['fetch_time','site_url','site_id','user_agent','emulated_as'])
df = pd.DataFrame([], columns=['URL','SEO','Accessibility','Performance','Best Practices'])
name = "Report_reduce_version" 
getdate = datetime.now().strftime("%m-%d-%y")
relative_path = 'C:\\Users\\mlope\\OneDrive\\Documentos\\Lighthouse\\Lighthouse_working\\assets\\'  ### WINDOWS -> \\..\\..\\

#Select URLs from input table
df_input_table = client.query('''
  SELECT 
    * 
  FROM `dbt-project-335000.dbt_mangel.input_test`''').result().to_dataframe()

def extract_info(preset):
    global df
    for ind in df_input_table.index:
    # print(df['url'][ind], df['id'][ind])   
        url = df_input_table['url'][ind]
        stream = os.popen('lighthouse --disable-storage-reset=true --preset=' +
                          preset + ' --output=json --output-path='+relative_path + name+'_'+getdate+'.report.json ' + url)

        time.sleep(60)
        print("INFO:Report complete for: " + url+' !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')

        json_filename = join(relative_path+name+ '_' +getdate + '.report.json ')

        with open(json_filename, encoding="utf8") as json_data:
            loaded_json = json.load(json_data)

        seo = str(round(loaded_json["categories"]["seo"]["score"] * 100))
        accessibility = str(round(loaded_json["categories"]["accessibility"]["score"] * 100))
        performance = str(round(loaded_json["categories"]["performance"]["score"] * 100))
        best_practices = str(round(loaded_json["categories"]["best-practices"]["score"] * 100))

        print("INFO:"+ seo + '!!!!!!!!!!!!!!!!!!!!!!!!!')
        print("INFO:"+ accessibility + '!!!!!!!!!!!!!!!!!!!!!!!!!')
        print("INFO:"+ performance + '!!!!!!!!!!!!!!!!!!!!!!!!!')
        print("INFO:"+ best_practices + '!!!!!!!!!!!!!!!!!!!!!!!!!')

        dict = {"URL":url,"SEO":seo,"Accessibility":accessibility,"Performance":performance,"Best Practices":best_practices}
        
        df = df.append(dict, ignore_index=True).sort_values(by='SEO', ascending=False)
        #print('INFO: df' +df+ ' !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
    df.to_csv(relative_path + name + '_' + getdate + '.csv')
    print(df)

extract_info(preset='desktop')





















##Get the values from the input table
#sql = """
#    SELECT 
#    * 
#  FROM `dbt-project-335000.dbt_mangel.input_test`;
#"""
#
## Start the query, passing in the extra configuration.
##query_job = client.query(sql, job_config=job_config)  # Make an API request.
##query_job.result()  # Wait for the job to complete.
#
#
#print("Query results loaded to the table {}".format(table_id_output))
#
#datasets = list(client.list_datasets())  # Make an API request.
#project = client.project
#
#if datasets:
#    print("Datasets in project {}:".format(project))
#    for dataset in datasets:
#        print("\t{}".format(dataset.dataset_id))
#else:
#    print("{} project does not contain any datasets.".format(project))

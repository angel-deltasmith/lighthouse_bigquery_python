
import os
import json
import os
import pandas as pd
import time
import datetime

from google.cloud.bigquery.client import Client
from google.cloud import bigquery
from datetime import datetime
from os.path import join
from google.cloud import bigquery

#BigQuery Connection
#Credentials
credential_path  ="C:\\Users\\mlope\\OneDrive\\Documentos\\Lighthouse\\Lighthouse_working\\dbtkey.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path

#Client
client = Client.from_service_account_json(credential_path, project='dbt-project-335000')

#Output_Table
#output_table = 'dbt-project-335000.dbt_mangel.data_feed'
output_table = client.get_table('dbt-project-335000.dbt_mangel.data_feed2')
print(output_table)

#Setup for writting in the table
job_config = bigquery.QueryJobConfig(destination=output_table)#parameters = destination table to write 
job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND #If the table already exists, then append the rows

#Variables
df = pd.DataFrame([], columns=['fetch_time','site_url','site_id','user_agent','emulated_as'])
#df = pd.DataFrame([], columns=['fetch_time','site_url','site_id','user_agent','emulated_as','accessibility'{'total_score','bypass_repetitive_content'},])

#df = pd.DataFrame([], columns=['URL','SEO','Accessibility','Performance','Best Practices'])
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
        url = df_input_table['url'][ind]
        id = df_input_table['id'][ind]
        stream = os.popen('lighthouse --disable-storage-reset=true --preset=' +
                          preset + ' --output=json --output-path='+relative_path + name+'_'+getdate+'.report.json ' + url)

        time.sleep(60)
        print("INFO:Report complete for: " + url+' !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')

        json_filename = join(relative_path+name+ '_' +getdate + '.report.json ')

        with open(json_filename, encoding="utf8") as json_data:
            loaded_json = json.load(json_data)

        fetch_time = loaded_json['fetchTime']
        site_url = loaded_json['finalUrl']
        site_id = id
        user_agent = loaded_json['userAgent']
        emulated_as = loaded_json['configSettings']['formFactor']

        print("INFO:"+ fetch_time + '!!!!!!!!!!!!!!!!!!!!!!!!!')
        print("INFO:"+ site_url + '!!!!!!!!!!!!!!!!!!!!!!!!!')
        print("INFO:ID !!!!!!!!!!!!!!!!!!!!!!!!!")
        print(site_id)
        print("INFO:"+ user_agent + '!!!!!!!!!!!!!!!!!!!!!!!!!')
        print("INFO:"+ emulated_as + '!!!!!!!!!!!!!!!!!!!!!!!!!')

        dict = {"fetch_time":fetch_time,"site_url":site_url,"site_id":site_id,"user_agent":user_agent,"emulated_as":emulated_as}
        df = df.append(dict, ignore_index=True).sort_values(by='site_id', ascending=False)

    #Load information to Big Query output table
    load_job = client.insert_rows_from_dataframe(table = output_table,dataframe = df, chunk_size = 500)  # API request

extract_info(preset='desktop')


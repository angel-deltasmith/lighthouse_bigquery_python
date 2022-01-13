import os
import json
import os
import pandas as pd
import time
import tempfile
import datetime
import validators 

from google.cloud import storage
from google.oauth2 import service_account
from google.cloud.bigquery.client import Client
from google.cloud import bigquery
from datetime import datetime
from os.path import join
from google.cloud import bigquery



angel_key = "String key comming from Airflow"

json_updated_angel = angel_key.replace("'", "\"")
json_angel_key= json.loads(json_updated_angel, strict=False)

#Credentials to connect to google storage
credentials = service_account.Credentials.from_service_account_info(json_angel_key)
client_data_feed = bigquery.Client(credentials=credentials,project='dbt-project-335000')
output_table = client_data_feed.get_table('dbt-project-335000.dbt_mangel.data_feed_complete')
#credentials = service_account.Credentials.from_service_account_info(json_angel_key)
client = storage.Client(project='dbt-project-335000', credentials=credentials)
bucket = client.get_bucket('lighthouse-daily-reports-test')
#blob = bucket.get_blob('Lighthouse_Arctic__desktop_01-06-22.report.json')
#json_data_string = blob.download_as_string().decode('utf8')
#json_data_dict = json.loads(json_data_string)

presets = ['desktop']
getdate = datetime.now().strftime("%m-%d-%y")

df_input_table = client_data_feed.query('''
  SELECT 
    * 
  FROM `dbt-project-335000.dbt_mangel.input_test`''').result().to_dataframe()


preset ='desktop'

def map_json(loaded_string,id):
  """Maps the JSON file coming from LIGHTHOUSE with the JSON that will be sent to BQ

  Args:
      file_name (string): Needs the name of the file that was created in the LIGHTHOUSE service
      id (string): the name of the company
  Returns:
      data_feed_json: returns the json file mapped
      [boolean]: returns a false if it was an error]
  """
  loaded_json = json.dumps(loaded_string)
  try:
    #json_filename = join(relative_path+file_name+ '_' +getdate + '.report.json ')
    #loaded_string = r + string con enter
    #loaded_json = json.loads(loaded_string)
    data_feed_json = [{
        'fetch_time' : loaded_json['fetchTime'],
        'site_url' : loaded_json['finalUrl'],
        'site_id' : id,
        'user_agent' :loaded_json['userAgent'],
        'emulated_as' : loaded_json['configSettings']['formFactor']

    }]
    return data_feed_json
  except AttributeError as error:
        print("AttributeError: {0}".format(error))
        return False
  except Exception as exception:
      # Output unexpected Exceptions.
      print("Exception: {0}".format(exception))
      return False
      
  except UnboundLocalError as unboundLocalError:
      # Output unexpected Exceptions.
      print("UnboundLocalError: {0}".format(unboundLocalError))
      return False

#Lighthouse's function
def run_lighthouse(preset, id,file_name, url):
  """Runs the lighthouse service, calls the map_json method and sends the information to BQ

  Args:
      preset (string): the preset that will be used to generate the report, could be perf(mobile) or desktop
      id (string): the name of the company
      file_name (string): the name of the file that was created in the LIGHTHOUSE service
      url (string): url used to call LIGHTHOUSE service

  Returns:
      [boolean]: [returns true if all the processes were executed correctly, returns a false if it was an error]
  """
  complete_file_name = file_name+'_'+getdate+'.report.json '
  print('------------------------------------------------------------------------------------------')
  print('INFO: URL: '+url)

  try:
    temp = tempfile.NamedTemporaryFile(prefix="complete_file_name")
    lighthouse_call = os.popen('lighthouse '+url+' --chrome-flags="--headless" --only-audits=bypass,color-contrast,document-title,duplicate-id-active,duplicate-id,html-has-lang,html-lang-valid,image-alt,label,link-name,list,listitem,'+
    'meta-viewport,is-on-https,uses-http2,uses-passive-event-listeners,no-document-write,external-anchors-use-rel-noopener,geolocation-on-start,doctype,no-vulnerable-libraries,notification-on-start,deprecations,password-inputs-can-be-pasted-into,errors-in-console,image-aspect-ratio,'
    +'first-contentful-paint,first-meaningful-paint,speed-index,interactive,first-cpu-idle,'+
    'load-fast-enough-for-pwa,works-offline,installable-manifest,redirects,viewport,service-worker,without-javascript,splash-screen,themed-omnibox,'+
    'meta-description,http-status-code,link-text,is-crawlable,robots-txt,hreflang,font-size,plugins '+
    '--disable-storage-reset=true --preset=' + preset + ' --output=json --output-path='+temp.name)

    time.sleep(60)
        #print(temp.read())
    json_lighthouse=json.load(temp)

    #print(profiles['fetchTime'])
    # Creates a new bucket and uploads an object
    json_to_send = json.dumps(json_lighthouse)
    new_blob = bucket.blob(complete_file_name)
    new_blob.upload_from_string(json_to_send)

    print('------------------------------------------------------------------------------------------')
    print("INFO:Report complete for: " + url+' !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
    print('------------------------------------------------------------------------------------------')
    data_feed_json_mapped = map_json(json_lighthouse,id)
    #data_feed_json_mapped = False
    if data_feed_json_mapped == False:
      print("no se escribio en BQ")
      return False
    else:
      df = pd.json_normalize(data_feed_json_mapped)
      print("Process finished calling LH and mapping the JSON")
      load_job = client.insert_rows_from_dataframe(table = output_table,dataframe = df, chunk_size = 500) #sends he informton to the data_feed table in BQ
      return True


  except AttributeError as error:
      print("AttributeError: {0}".format(error))
  except Exception as exception:
      # Output unexpected Exceptions.
      print("Exception: {0}".format(exception))
      
  except UnboundLocalError as unboundLocalError:
      # Output unexpected Exceptions.
      print("UnboundLocalError: {0}".format(unboundLocalError))

def validations(preset,df_input_table):
  """This method validates if the urls coming from BQ have the right format, creates some variables that will be send to the lighthouse service and 
  calls the run_lighthouse method
  Args:
      preset (string): preset (string): the preset that will be used to generate the report, could be perf(mobile) or desktop
      df_input_table (table): the input table from BQ that has the information from the companies

  Returns:
      [boolean]: [returns true if all the processes were executed correctly, returns a false if it was an error]
  """
  for ind in df_input_table.index:
    url = df_input_table['url'][ind]
    valid_url = validators.url(url)
    id = df_input_table['name'][ind]

    if preset == 'perf':
      preset_name = 'mobile'
    else:
      preset_name = preset

    company_name = id.partition(" ")[0]
    file_name = 'Lighthouse_'+ company_name +'_' + '_' + preset_name

    if valid_url == True:
     service_run= run_lighthouse(preset, id,file_name, url)
    else:
      if not '.' in url:# if the dot is missing the function won't call the API
            print("Invalid url") 
            service_run = False
      elif not 'https://' in url:#adds the https if the url doesn't have it
            url = 'https://' + str(url)
            service_run= run_lighthouse(preset, id,file_name, url)
  
  return service_run


#def run_code():
for preset in presets:
  validations(preset,df_input_table)















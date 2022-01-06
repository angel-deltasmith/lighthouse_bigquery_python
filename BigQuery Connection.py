import os
import json
import os
import pandas as pd
import time
import datetime
import validators 

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
output_table = client.get_table('dbt-project-335000.dbt_mangel.data_feed_complete')
print('------------------------------------------------------------------------------------------')
print('INFO OUTPUT TABLE:')
print(output_table)
print('------------------------------------------------------------------------------------------')
#Setup for writting in the table
job_config = bigquery.QueryJobConfig(destination=output_table)#parameters = destination table to write 
job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND #If the table already exists, then append the rows

#Variables
getdate = datetime.now().strftime("%m-%d-%y")
relative_path = 'C:\\Users\\mlope\\OneDrive\\Documentos\\Lighthouse\\Lighthouse_working\\assets\\'  ### WINDOWS -> \\..\\..\\
presets = ['desktop','perf']

#Select URLs from input table
df_input_table = client.query('''
  SELECT 
    * 
  FROM `dbt-project-335000.dbt_mangel.input_test`''').result().to_dataframe()


def map_json(complete_file_name,id):
  """Maps the JSON file coming from LIGHTHOUSE with the JSON that will be sent to BQ

  Args:
      file_name (string): Needs the name of the file that was created in the LIGHTHOUSE service
      id (string): the name of the company
  Returns:
      data_feed_json: returns the json file mapped
      [boolean]: returns a false if it was an error]
  """
  try:
    #json_filename = join(relative_path+file_name+ '_' +getdate + '.report.json ')
    with open(complete_file_name, encoding="utf8") as json_data:
      loaded_json = json.load(json_data)
      
      data_feed_json = [{
        'fetch_time' : loaded_json['fetchTime'],
        'site_url' : loaded_json['finalUrl'],
        'site_id' : id,
        'user_agent' :loaded_json['userAgent'],
        'emulated_as' : loaded_json['configSettings']['formFactor'],
        'accessibility': [{
          'total_score': loaded_json['categories']['accessibility']['score'],
          'bypass_repetitive_content': loaded_json['audits']['bypass']['score'] == 1,
          'color_contrast': loaded_json['audits']['color-contrast']['score'] == 1,
          'document_title_found': loaded_json['audits']['document-title']['score'] == 1,
          'no_duplicate_id_attribute': loaded_json['audits']['duplicate-id-active']['score'] == 1,#['duplicate-id']['score'] is 1,
          'html_has_lang_attribute': loaded_json['audits']['html-has-lang']['score'] == 1,
          'html_lang_is_valid': loaded_json['audits']['html-lang-valid']['score'] == 1,
          'images_have_alt_attribute': loaded_json['audits']['image-alt']['score'] == 1,
          'form_elements_have_labels': loaded_json['audits']['label']['score'] == 1,
          'links_have_names': loaded_json['audits']['link-name']['score'] == 1,
          'lists_are_well_formed': loaded_json['audits']['list']['score'] == 1,
          'list_items_within_proper_parents': loaded_json['audits']['listitem']['score'] == 1,
          'meta_viewport_allows_zoom': loaded_json['audits']['meta-viewport']['score'] == 1
          }],
        'best_practices': [{
          'total_score': loaded_json['categories']['best-practices']['score'],
          'avoid_application_cache': True,#loaded_json['audits']['appcache-manifest']['score'] is 1,
          'uses_https': loaded_json['audits']['is-on-https']['score'] == 1,
          'uses_http2': loaded_json['audits']['uses-http2']['score'] == 1, 
          'uses_passive_event_listeners': loaded_json['audits']['uses-passive-event-listeners']['score'] == 1,
          'no_document_write': loaded_json['audits']['no-document-write']['score'] == 1,
          'external_anchors_use_rel_noopener': True, #loaded_json['audits']['external-anchors-use-rel-noopener']['score'] is 1,
          'no_geolocation_on_start': loaded_json['audits']['geolocation-on-start']['score'] == 1,
          'doctype_defined': loaded_json['audits']['doctype']['score'] == 1,
          'no_vulnerable_libraries': loaded_json['audits']['no-vulnerable-libraries']['score'] == 1,
          'notification_asked_on_start': loaded_json['audits']['notification-on-start']['score'] == 1,
          'avoid_deprecated_apis': loaded_json['audits']['deprecations']['score'] == 1,
          'allow_paste_to_password_field': loaded_json['audits']['password-inputs-can-be-pasted-into']['score'] == 1,
          'errors_in_console': loaded_json['audits']['errors-in-console']['score'] == 1,
          'images_have_correct_aspect_ratio': loaded_json['audits']['image-aspect-ratio']['score'] == 1
        }],
        'performance': [{
          'total_score': loaded_json['categories']['performance']['score'],
          'first_contentful_paint': [{
            'raw_value':loaded_json['audits']['first-contentful-paint']['numericValue'] ,#loaded_json['audits']['first-contentful-paint']['rawValue'],
            'score': loaded_json['audits']['first-contentful-paint']['score']
          }],
          'first_meaningful_paint': [{
            'raw_value': loaded_json['audits']['first-meaningful-paint']['numericValue'],#['rawValue'],
            'score': loaded_json['audits']['first-meaningful-paint']['score']
          }],
          'speed_index': [{
            'raw_value': loaded_json['audits']['speed-index']['numericValue'],#['rawValue'],
            'score': loaded_json['audits']['speed-index']['score']
          }],
          'page_interactive': [{
            'raw_value': loaded_json['audits']['interactive']['numericValue'],#['rawValue'],
            'score': loaded_json['audits']['interactive']['score']
          }],
          'first_cpu_idle': [{
            'raw_value': 1, #loaded_json['audits']['first-cpu-idle']['numericValue']
            'score': 1, #loaded_json['audits']['first-cpu-idle']['score']
          }]
        }],
        'pwa': [{
          'total_score': loaded_json['categories']['pwa']['score'],
          'load_fast_enough': True ,#loaded_json['audits']['load-fast-enough-for-pwa']['score'] is 1,
          'works_offline': True,#loaded_json['audits']['works-offline']['score'] is 1,
          'installable_manifest': loaded_json['audits']['installable-manifest']['score'] == 1,
          'uses_https': loaded_json['audits']['is-on-https']['score'] == 1,
          'redirects_http_to_https': loaded_json['audits']['redirects']['score'] == 1,#['redirects-http']['score'] is 1,
          'has_meta_viewport': loaded_json['audits']['viewport']['score'] == 1,###is in both, mobile and desktop
          'uses_service_worker': loaded_json['audits']['service-worker']['score'] == 1,
          'works_without_javascript': True ,#loaded_json['audits']['without-javascript']['score'] is 1,
          'splash_screen_found': loaded_json['audits']['splash-screen']['score'] == 1,
          'themed_address_bar': loaded_json['audits']['themed-omnibox']['score'] == 1
        }],
        'seo': [{
          'total_score': loaded_json['categories']['seo']['score'],
          'has_meta_viewport': loaded_json['audits']['viewport']['score'] == 1,
          'document_title_found': loaded_json['audits']['document-title']['score'] == 1,
          'meta_description': loaded_json['audits']['meta-description']['score'] == 1,
          'http_status_code': loaded_json['audits']['http-status-code']['score'] == 1,
          'descriptive_link_text': loaded_json['audits']['link-text']['score'] == 1,
          'is_crawlable': loaded_json['audits']['is-crawlable']['score'] == 1,
          'robots_txt_valid': loaded_json['audits']['robots-txt']['score'] == 1,
          'hreflang_valid': loaded_json['audits']['hreflang']['score'] == 1,
          'font_size_ok': loaded_json['audits']['font-size']['score'] == 1,
          'plugins_ok': loaded_json['audits']['plugins']['score'] == 1
        }]

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


#Lighthouse function
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
  complete_file_name = relative_path + file_name+'_'+getdate+'.report.json '
  print('------------------------------------------------------------------------------------------')
  print('INFO: URL: '+url)
  print('INFO: ID: ')
  print(id)
  print('------------------------------------------------------------------------------------------')
  try:
    lighthouse_call = os.popen('lighthouse --only-audits=bypass,color-contrast,document-title,duplicate-id-active,duplicate-id,html-has-lang,html-lang-valid,image-alt,label,link-name,list,listitem,'+
    'meta-viewport,is-on-https,uses-http2,uses-passive-event-listeners,no-document-write,external-anchors-use-rel-noopener,geolocation-on-start,doctype,no-vulnerable-libraries,notification-on-start,deprecations,password-inputs-can-be-pasted-into,errors-in-console,image-aspect-ratio,'
    +'first-contentful-paint,first-meaningful-paint,speed-index,interactive,first-cpu-idle,'+
    'load-fast-enough-for-pwa,works-offline,installable-manifest,redirects,viewport,service-worker,without-javascript,splash-screen,themed-omnibox,'+
    'meta-description,http-status-code,link-text,is-crawlable,robots-txt,hreflang,font-size,plugins '+
    '--disable-storage-reset=true --preset=' + preset + ' --output=json --output-path='+complete_file_name + url )

    time.sleep(60)
    print('------------------------------------------------------------------------------------------')
    print("INFO:Report complete for: " + url+' !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
    print('------------------------------------------------------------------------------------------')
    data_feed_json_mapped = map_json(complete_file_name,id)

    if data_feed_json_mapped == False:
      print("no se escribio en BQ")
      return False
    else:
      df = pd.json_normalize(data_feed_json_mapped)
      print("doneeeeeeeeeee")
      load_job = client.insert_rows_from_dataframe(table = output_table,dataframe = df, chunk_size = 500)  # API request
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
  """[summary]

  Args:
      preset ([type]): [description]
      df_input_table ([type]): [description]

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
      
      elif not 'https://' in url:#adds the https if the url doesn't have it
            url = 'https://' + str(url)
            print('pase por el if')
            service_run= run_lighthouse(preset, id,file_name, url)
    print("INFO SERVICE!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    print(service_run)        
  return service_run

for preset in presets:
  validations(preset,df_input_table)


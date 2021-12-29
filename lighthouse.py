import json
import os
import time
#from numpy.lib.function_base import extract
from datetime import datetime
from os.path import join
from openpyxl import Workbook
wb1 = Workbook()
wb2 = Workbook()

### create needed variables

name = "Report"
getdate = datetime.now().strftime("%m-%d-%y")

### set relative_path MAC/WINDOWS

relative_path = 'C:\\Users\\mlope\\OneDrive\\Documentos\\Lighthouse\\Lighthouse_working\\assets\\'  ### WINDOWS -> \\..\\..\\
# relative_path = '/Users/seuleanolimpi/Desktop/PythonReport/' ### MAC -> /../../

csv_file_mob = join(relative_path, 'lighthouse_mobile_' + getdate + '.csv')
csv_file_des = join(relative_path, 'lighthouse_desktop_' + getdate + '.csv')

ws_mob = wb1.active;
ws_des = wb2.active;


def last_row_mob(): return len(ws_mob['A'])
def last_row_des(): return len(ws_des['A'])

### initialize an array with the links you want to run the Lighthouse script on

urls = [
"https://arctictravelcompany.com/",
"http://www.fjellheisen.no"
]

### set the 'base' object, in Python AKA - dictionary; for setting the Header forEach iteration in Excel based on the num_of_call
### if the value is getting higher than 6 -> eg: num_of_calls = 7, then you need to create another key-value -> 7: 'Seventh Run',
base = {
    1: 'First Run',
    2: 'Second Run',
    3: 'Third Run',
    4: 'Fourth Run',
    5: 'Fifth Run',
    6: 'Sixth Run',
}

def extract_info(run, preset):

    header = [run, 'Product_Name', 'URL', 'First_Contentful_Paint', 'Speed_Index', 'Largest_Contentful_Paint', 'Performance']
    if preset == 'desktop':     ### preset -> 2 values: 'desktop' & 'perf'(for mobile)
        last = last_row_des()+1
        working = ws_des
    else:
        last = last_row_mob()+1
        working = ws_mob

    if 'first' not in run.lower():
        last += 1

    for i, r in enumerate('ABCDEFG'):

        working[r+str(last)].value = header[i]

    for url in urls:
        print('INFO: Lighthouse call Opened!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
        stream = os.popen('lighthouse --disable-storage-reset=true --preset=' +
                          preset + ' --output=json --output-path='+relative_path + name+'_'+getdate+'.report.json ' + url)
        print('INFO: Lighthouse call finished!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
        #print('lighthouse --disable-storage-reset=true --preset=' +preset + ' --output=json --output-path='+relative_path + name+'_'+getdate+'.report.json ' + url)
        time.sleep(60)
        json_filename = join(relative_path+name+ '_' +getdate + '.report.json ')
        #print(relative_path+name+ '_' +getdate + '.report.json')
        ### open the JSON FILE and start processing it

        print('INFO: Creating JSON FILE!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
        with open(json_filename, encoding="utf8") as json_data:
            loaded_json = json.load(json_data)
            
                          
        print('INFO: JSON Created!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')

        ### set the items you need from the JSON FILE here
        try: 
            Product_Name = loaded_json["audits"]["largest-contentful-paint-element"]["details"]["items"][0]["node"]["nodeLabel"] ### get the name of the product to be descriptive
            First_Contentful_Paint = str(round(loaded_json["audits"]["first-contentful-paint"]["score"] * 100))
            Speed_Index = str(round(loaded_json["audits"]["speed-index"]["score"] * 100))
            Largest_Contentful_Paint = str(round(loaded_json["audits"]["largest-contentful-paint"]["score"] * 100))
            Performance = str(round(loaded_json["categories"]["performance"]["score"] * 100))
        except Exception as e:
            Product_Name = First_Contentful_Paint = Speed_Index = Largest_Contentful_Paint = Performance = '-'
            print(e)

        ### (1) if you want to add a new column for the report you need to create a new var as fcps, fcpdv for eg. (see below) -- these are coming from JSON FILE
        ### (2) go to the line, where the 'header' AKA dataFrame is initialised (above all - even under the declaration of the function) and add the new item as a string for eg: [..., 'lcpdv']
        ### (3) go to the line where the enumeration was set and add another letter which corresponds with the next column in excel [...,JKLMN...]
        ### (4) go to the line where we set the 'header' for the excel report -> data: [urls.index(url),...., lcpdv]
        ### (5) go to the line where the enumeration was set and add another letter which corresponds with the next column in excel [...,JKLMN...]

        ### if you increase the columns for your report, don't forget to add them below -> data = [..., sidv] !!! steps -> (4) - (5)
        data = [urls.index(url), Product_Name, url, First_Contentful_Paint, Speed_Index, Largest_Contentful_Paint, Performance]
        if preset == 'desktop':
            last = last_row_des()+1
        else:
            last = last_row_mob()+1
        for i, r in enumerate('ABCDEFG'):
            working[r+str(last)].value = data[i]

### here you can set how many times to run the test on the links

num_of_call = 1
for i in range(1, num_of_call+1):
    #extract_info(base[i], preset='perf') ### run the test on mobile
    extract_info(base[i], preset='desktop') ### run the test on desktop

wb1.save(csv_file_mob)  ### save the results for mobile in an EXCEL FILE
wb2.save(csv_file_des) ### save the results for desktop in an EXCEL FILE
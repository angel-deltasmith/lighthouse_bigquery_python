
"""
Example DAG demonstrating the usage of the TaskFlow API to execute Python functions natively and within a
virtual environment.
"""
import logging
import shutil
import time
from datetime import datetime
from pprint import pprint

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonVirtualenvOperator

log = logging.getLogger(__name__)

with DAG(
    dag_id='dev_example_venv',
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:
    # [START howto_operator_python]
    @task(task_id="print_the_context")
    def print_context(ds=None, **kwargs):
        """Print the Airflow context and ds variable from the context."""
        pprint(kwargs)
        print(ds)
        return 'Whatever you return gets printed in the logs'

    run_this = print_context()
    # [END howto_operator_python]

    # [START howto_operator_python_kwargs]
    # Generate 5 sleeping tasks, sleeping from 0.0 to 0.4 seconds respectively
    for i in range(5):

        @task(task_id=f'sleep_for_{i}')
        def my_sleeping_function(random_base):
            """This is a function that will run within the DAG execution"""
            time.sleep(random_base)

        sleeping_task = my_sleeping_function(random_base=float(i) / 10)

        run_this >> sleeping_task
    # [END howto_operator_python_kwargs]

    if not shutil.which("virtualenv"):
        log.warning("The virtalenv_python example task requires virtualenv, please install it.")
    else:
        # [START howto_operator_python_venv]
        @task.virtualenv(
            task_id="virtualenv_python", requirements=["colorama==0.4.0","lighthouse","node","cachetools==4.2.4","certifi==2021.10.8","charset-normalizer==2.0.9","crcmod==1.7","dataclasses-json==0.3.6","DateTime==4.3","decorator==5.1.0","distro-info","et-xmlfile==1.1.0","export==0.1.2","google-api-core==2.3.2","google-auth==2.3.3","google-cloud-bigquery==2.31.0","google-cloud-bigquery-connection==1.3.1","google-cloud-core==2.2.1","google-cloud-storage==2.0.0","google-crc32c==1.3.0","google-resumable-media==2.1.0","googleapis-common-protos==1.54.0",
            "grpc-google-iam-v1==0.12.3",
            "grpcio==1.43.0",
            "grpcio-status==1.43.0",
            "idna==3.3",
            "join==0.1.1",
            "marshmallow==3.14.1",
            "marshmallow-enum==1.5.1",
            "mypy-extensions==0.4.3",
            "numpy==1.21.5",
            "openpyxl==3.0.9",
            "packaging==21.3",
            "pandas==1.3.5",
            "popen==0.1.20",
            "proto-plus==1.19.8",
            "protobuf==3.19.1",
            "pyarrow==6.0.1",
            "pyasn1==0.4.8",
            "pyasn1-modules==0.2.8",
            "pyparsing==3.0.6",
            "python-dateutil==2.8.2",
            "pytz==2021.3",
            "requests==2.26.0",
            "rsa==4.8",
            "six==1.16.0",
            "stringcase==1.2.0",
            "typing-inspect==0.7.1",
            "typing_extensions==4.0.1",
            "urllib3==1.26.7",
            "validators==0.18.2",
            "zope.interface==5.4.0"], system_site_packages=False
        )
        def callable_virtualenv():
            """
            Example function that will be performed in a virtual environment.

            Importing at the module level ensures that it will not attempt to import the
            library before it is installed.
            """
            import os
            import json
            import os
            import pandas as pd
            import time
            import tempfile
            import datetime
            import validators 
            from time import sleep

            temp = tempfile.NamedTemporaryFile(prefix="tempfile")
            url = "https://arctictravelcompany.com/"
            print('------------------------------------------------------------------------------------------')
            print("INFO:CALLING LIGHTHOUSE FOR: " + url)
            print('------------------------------------------------------------------------------------------')
            
            preset = "desktop"

            lighthouse_call = os.popen('lighthouse '+url+' --chrome-flags="--no-sandbox --headless" --only-audits=bypass,color-contrast,document-title,duplicate-id-active,duplicate-id,html-has-lang,html-lang-valid,image-alt,label,link-name,list,listitem,'+
            'meta-viewport,is-on-https,uses-http2,uses-passive-event-listeners,no-document-write,external-anchors-use-rel-noopener,geolocation-on-start,doctype,no-vulnerable-libraries,notification-on-start,deprecations,password-inputs-can-be-pasted-into,errors-in-console,image-aspect-ratio,'
            +'first-contentful-paint,first-meaningful-paint,speed-index,interactive,first-cpu-idle,'+
            'load-fast-enough-for-pwa,works-offline,installable-manifest,redirects,viewport,service-worker,without-javascript,splash-screen,themed-omnibox,'+
            'meta-description,http-status-code,link-text,is-crawlable,robots-txt,hreflang,font-size,plugins '+
            '--disable-storage-reset=true --preset='+preset+' --output=json --output-path='+temp.name)
            
            time.sleep(30)

            print('------------------------------------------------------------------------------------------')
            print("INFO:REPORT COMPLETED FOR: " + url)
            print('------------------------------------------------------------------------------------------')

            json_temp=json.load(temp)
            print(json_temp)

    
        virtualenv_task = callable_virtualenv()
        # [END howto_operator_python_venv]
### Use Instructions ###

1. Clone or fork this repository.
2. Please, have the following before running the script:

    * Python 3.X
    * Node.js on your local machine
    * npm install -g lighthouse
    * pip install openpyxl
    * pip install DateTime
    * pip install join
    * pip install export
    * pip install pyarrow
    * pip install --upgrade google-cloud-BigQuery
    * pip3 install --upgrade google-cloud-bigquery
    * pip install --upgrade google-cloud
    * pip install --upgrade google-cloud-bigquery
    * pip install --upgrade google-cloud-bigquery
    
3. Make sure to change the path for the key in order to connect to BQ (line 17)
4. Add the relative path where you want save the JSON files coming from lighthouse (line 36)
5. Launch the Python file lighthouse_API.py in Visual Code or any IDE and run the following command: `python name_of_the_file.py`
6. The code will fill the data_feed table in BQ with the information coming from lighthouse

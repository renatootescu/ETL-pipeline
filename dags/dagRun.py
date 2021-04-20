import json
import pandas as pd
import os
# import boto3
import datetime
import pymongo
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import psycopg2

# select the database system to be used: mongoDB (noSQL) or Amazon Redshift (RDBMS)
# database = 'mongoDB'
database = 'Redshift'

if database == 'mongoDB':
    # connect to the MONGO database
    # the mongo DB connection string
    client = pymongo.MongoClient('mongoDB_connection_string')
    # the database to be used
    db = client.testairflow

else:
    # Amazon Redshift database connection details
    # the details bellow can also be saved in environment variables
    dbname = 'testairflow'
    host = '*******************************.eu-central-1.redshift.amazonaws.com'
    port = '****'
    user = '*********'
    password = '********************'
    awsIAMrole = 'arn:aws:iam::************:role/*******'


def getDBdate(ti):
    """
    Args:
        ti: task instance argument used by Airflow to push and pull xcom data

    Pushes an xcom to the Airflow database with the date of the most recent db entry
    (An xcom is a tool used to share small amounts of data between Airflow dag tasks. Similar to Airflow variable, but
    unlike Airflow variables which are global and can be shared between multiple dags, xcom shares between the tasks
    of a dag.)
    """

    # try-except error handler: if the db aggregation fails, return None to not process the same data multiple times
    try:
        if database == 'mongoDB':
            # >>> use the MONGO database
            # Find the the latest database document
            # filter for the Mongo db aggregation: the key 'dateFor' has to be exist in the collection
            aggFilter = {'dateFor': {'$exists': True}}
            # in the collection/table (db.countyDiff), apply the filter to the aggregation, convert the dates
            # from str to datetime, sort descending and return one (the first one)
            dateDoc = list(db.countyDiff.aggregate([{'$match': aggFilter},
                                                    {'$project': {
                                                        'date': {
                                                            '$dateFromString': {
                                                                'dateString': '$dateFor',
                                                                'format': '%Y-%m-%d'}
                                                        }
                                                    }},
                                                    {'$sort': {'date': -1}},
                                                    {'$limit': 1}
                                                    ]))

            # try-except error handler to set base parsing start date if the aggregation above returns and empty
            # list (i.e.: no data found)
            try:
                fetchedDate = dateDoc[0]['date'].strftime('%Y-%m-%d')
            except:
                fetchedDate = '2020-01-01'

        else:
            # >>> use the AMAZON REDSHIFT database
            # set up the connection to the Redshift database
            conn = psycopg2.connect(f'dbname={dbname} host={host} port={port} user={user} password={password}')
            # start the database cursor
            cursor = conn.cursor()
            # grab the latest date from the counties collection
            sql = """SELECT dateFor FROM counties ORDER BY dateFor DESC LIMIT 1;"""
            # try-except error handler to return a parsing start date in case the cursor execute fails
            try:
                cursor.execute(sql)
                # convert the date from datetime to string
                fetchedDate = cursor.fetchall()[0][0].strftime('%Y-%m-%d')
                # close the connection and cursor
                cursor.close()
                conn.close()
            except:
                fetchedDate = '2020-01-01'
                cursor.close()
                conn.close()

    except:
        fetchedDate = None

    # push the task instance (key, value format) to an xcom
    ti.xcom_push(key='fetchedDate', value=fetchedDate)


def getLastDate(ti):
    """
    Pull the date from xcom and return one task id, or multiple task IDs if they are inside a list, to be executed
    Args:
        ti: task instance argument used by Airflow to push and pull xcom data

    Returns: the airflow task ID to be executed based on the xcom data
    """

    # pull the xcom data (in this case a list containing only one element: the date string or None)
    fetchedDate = ti.xcom_pull(key='fetchedDate', task_ids=['getLastProcessedDate'])
    # if the date is None then execute the 'parseJsonFile' task, else execute the 'endRun' task
    if fetchedDate[0] is not None:
        return 'parseJsonFile'
    return 'endRun'


def readJsonData(ti):
    """
    Read and parse a Json, save the parsed data to a CSV
    Args:
        ti: task instance argument used by Airflow to push and pull xcom data

    Returns: a task id to be executed
    """

    # get the date data from the xcom
    fetchedDate = ti.xcom_pull(key='fetchedDate', task_ids=['getLastProcessedDate'])
    # grab the first element (the date string) from the list pulled from xcom and convert it from string to datetime
    lastDBDate = datetime.datetime.strptime(fetchedDate[0], '%Y-%m-%d')

    # connect to s3 using the boto3 AWS python module (credentials are saved in an env variable)
    # set up the connection to the Amazon cloud bucket
    # s3 = boto3.client('s3')
    # get the file data from the S3 bucket
    # obj = s3.get_object(Bucket='renato-airflow-raw', Key='mar.json')
    # open the file in memory
    # filename = obj['Body'].read().decode('utf-8', errors='ignore')

    # connect to s3 using the Airflow hook system. Credentials are saved in env variables when the container is
    # built, using the AIRFLOW_CONN_AWS_DEFAULT environment flag in the docker-compose.yml file.
    # s3 hook object
    hook = S3Hook()
    # name of the file in the AWS s3 bucket
    key = 'countyData.json'
    # name of the AWS s3 bucket
    bucket = 'renato-airflow-raw'
    # directory in which the file will be saved
    path = '/opt/airflow/sparkFiles'
    # download the file
    filename = hook.download_file(
        key=key,
        bucket_name=bucket,
        local_path=path
    )

    # open the json data with the correct encoding. 'latin-1' encoding was used as the json contains some chars that
    # are not encoded in utf-8 (which is usually used)
    with open(filename, encoding='latin-1') as data:
        # load the data as a dictionary (key, value pairs)
        jsonData = json.load(data)
        # make sure that historicalData is in the dictionary keys
        if 'historicalData' in jsonData.keys():
            # empty list for saving the parsed data
            dfData = []
            # for each date (key) in the 'historicalData' dictionary
            for key in jsonData['historicalData'].keys():
                # if the last db date is smaller/earlier or equal with the date key
                if lastDBDate <= datetime.datetime.strptime(key, '%Y-%m-%d'):
                    # check if the value containing teh data is a dictionary. This way None values are skipped.
                    if type(jsonData["historicalData"][key]['countyInfectionsNumbers']) == dict:
                        # create a new empty dict for each date
                        parsedLine = {}
                        # save the date in the dict with the key 'dateFor'
                        parsedLine['dateFor'] = key
                        # update the new dict (add/append) with the required json data for each date
                        parsedLine.update(jsonData["historicalData"][key]['countyInfectionsNumbers'])
                        # save the new dict to the list created above
                        dfData.append(parsedLine)

            # check if the data list is not empty (has a length of 0)
            if len(dfData) > 0:
                # convert the list ot a Pandas dataframe
                df = pd.DataFrame(dfData)
                # replace NAN (None) values with 0
                df = df.fillna(0)
                # save the df to a csv with the headers added and with utf8 encoding
                df.to_csv('/opt/airflow/sparkFiles/parsedData.csv',
                          encoding='utf8',
                          index=False,
                          header=True)

                # delete the file downloaded from s3
                os.remove(filename)

                return 'processParsedData'
            return 'endRun'
        return 'endRun'


def uploadToDB(ti):
    """
    Upload the results data to the database
    """
    results = '/opt/airflow/sparkFiles/results.csv'

    # get the date from the xcom
    fetchedDate = ti.xcom_pull(key='fetchedDate', task_ids=['getLastProcessedDate'])
    lastDBDate = fetchedDate[0]

    # read the results CSV to a Pandas dataframe
    pandasDf = pd.read_csv(results)
    # remove the row that has the same dateFor as the previously last processed date to avoid any data errors
    newDf = pd.concat([pandasDf.loc[pandasDf.dateFor != lastDBDate]])

    if database == 'mongoDB':
        # >>> using the MONGO database

        # convert to a list of dictionary objects
        resultsList = newDf.to_dict(orient='records')
        # save the data to the database as bulk
        # insertToDB = db.countyDiff.insert_many(resultsList)

        # save data by replacing documents already found in the collection if the dateFor fields match, if not insert
        # new document by setting the upsert flag to True
        for item in resultsList:
            insertToDB = db.countyDiff.replace_one({'dateFor': item['dateFor']},
                                                   item,
                                                   upsert=True)

    else:
        # >>> using the AMAZON REDSHIFT database

        # overwrite the CSV with the new data
        newDf.to_csv(results,
                     sep=',',
                     header=True,
                     index=False)
        #upload results csv to S3
        # s3 hook object
        hook = S3Hook()
        # name of the file in the AWS s3 bucket
        key = 'results.csv'
        # name of the AWS s3 bucket
        bucket = 'renato-airflow-raw'
        # load/upload the results file to the s3 bucket
        loadToS3 = hook.load_file(
            filename=results,
            key=key,
            bucket_name=bucket,
            replace=True
        )

        # set up the connection to the Redshift database
        conn = psycopg2.connect(f'dbname={dbname} host={host} port={port} user={user} password={password}')
        # start the database cursor
        cursor = conn.cursor()
        # COPY the data from the s3 loaded file into the Redshift counties collection.
        # the COPY command only appends the CSV data to the table. It does not replace
        sql = f"""COPY counties FROM 's3://renato-airflow-raw/results.csv' 
                  iam_role '{awsIAMrole}' 
                  DELIMITER AS ',' 
                  DATEFORMAT 'YYYY-MM-DD' 
                  IGNOREHEADER 1 ;"""

        cursor.execute(sql)
        conn.commit()
        cursor.close()
        conn.close()

    # delete the parsed data csv from the working directory
    os.remove(results)


# set up DAG arguments
defaultArgs = {
    'owner': 'Renato_Otescu',
    'start_date': datetime.datetime(2021, 1, 1),
    'retries': 3,
    'retry_delay': datetime.timedelta(seconds=30)
}

# plan DAG run/pipeline (schedule_interval='0 8 * * *' for each day at 8AM or schedule_interval='@daily' for 0AM)
# the first argument is the name of the DAG: 'analyze_json_data'
with DAG('analyze_json_data',
         schedule_interval='@daily',
         default_args=defaultArgs,
         catchup=False) as dag:

    # task that calls the function getDBdate by using a PythonOperator
    getLastProcessedDate = PythonOperator(
        task_id='getLastProcessedDate',
        python_callable=getDBdate
    )

    # Call the function getLastDate. A BranchPythonOperator is used here as the getLastDate function returns either
    # the 'parseJsonFile' task id or the 'endRun' task id.
    # 2 branches are created: one for the task 'parseJsonFile' and the other one for the task id 'endRun'.
    # If multiple tasks need to be executed at the same time, the return of the function has to be a list containing
    # all the tasks ids that need to be executed at the same time (i.e.: if ['task_id_1', 'task_id_2', etc])
    # The flag do_xcom_push is set to False because each xcom also creates a separate data point in the Airflow DB
    # which in this case is useless
    getDate = BranchPythonOperator(
        task_id='getDate',
        python_callable=getLastDate,
        do_xcom_push=False
    )

    # if the BranchPythonOperator above returns the task id 'parseJsonFile', then the readJsonData function is called
    # note that the 'parseJsonFile' task is also a BranchPythonOperator because the function it calls, readJsonData,
    # also returns 2 task ids: 'processParsedData' and 'endRun'
    parseJsonFile = BranchPythonOperator(
        task_id='parseJsonFile',
        python_callable=readJsonData,
        do_xcom_push=False
    )

    # as the Spark (PySpark) script is located in a different directory, it is executed using a BashOperator
    processParsedData = BashOperator(
        task_id='processParsedData',
        bash_command='python /opt/airflow/sparkFiles/sparkProcess.py'
    )

    # execute the 'uploadToDB' function using a PythonOperator
    saveToDB = PythonOperator(
        task_id='saveToDB',
        python_callable=uploadToDB
    )

    # the last task is a DummyOperator used only to point the branch operators above
    # The trigger rule 'none_failed_or_skipped' ensures that the dummy task is executed if at least one parent succeeds
    endRun = DummyOperator(
        task_id='endRun',
        trigger_rule='none_failed_or_skipped'
    )

    # set tasks relations (the order the tasks are executed)
    getLastProcessedDate >> getDate
    # the task 'getDate' is a BranchPythonOperator so after it 2 other tasks can be executed parseJsonFile and endRun
    getDate >> [parseJsonFile, endRun]
    # same as above for the branch parseJsonFile
    parseJsonFile >> [processParsedData, endRun]
    processParsedData >> saveToDB >> endRun

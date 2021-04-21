<h1 align="center">ETL Pipeline with Airflow, Spark, s3, MongoDB and Amazon Redshift</h1>

<p align="center">
  <a href="#about">About</a> •
  <a href="#scenario">Scenario</a> •
  <a href="#base-concepts">Base Concepts</a> •
  <a href="#prerequisites">Prerequisites</a> •
  <a href="#set-up">Set-up</a> •
  <a href="#installation">Installation</a> •
  <a href="#airflow-interface">Airflow Interface</a> •
  <a href="#pipeline-task-by-task">Pipeline Task by Task</a> •
  <a href="#shut-down-and-restart-airflow">Shut Down and Restart Airflow</a> •
  <a href="#learning-resources">Learning Resources</a>
</p>

---

## About

Educational project on how to build an ETL (Extract, Transform, Load) data pipeline, orchestrated with Airflow.

An AWS s3 bucket is used as a Data Lake in which json files are stored. The data is extracted from a json and parsed (cleaned). It is then transformed/processed with Spark (PySpark) and loaded/stored in either a Mongodb database or in an Amazon Redshift Data Warehouse.

The pipeline architecture - author's interpretation:

<p align="center"><img src=https://user-images.githubusercontent.com/19210522/115540283-9b609100-a2a6-11eb-9f48-08f3a17528d8.png></p>

#### Note: Since this project was built for learning purposes and as an example, it functions only for a single scenario and data schema.

The project is built in Python and it has 2 main parts:
  1. The Airflow DAG file, [**dags/dagRun.py**](https://github.com/renatootescu/ETL-pipeline/blob/main/dags/dagRun.py), which orchestrates the data pipeline tasks.
  2. The PySpark data transformation/processing script, located in [**sparkFiles/sparkProcess.py**](https://github.com/renatootescu/ETL-pipeline/blob/main/sparkFiles/sparkProcess.py)

#### Note: The code and especially the comments in the python files [**dags/dagRun.py**](https://github.com/renatootescu/ETL-pipeline/blob/main/dags/dagRun.py) and [**sparkFiles/sparkProcess.py**](https://github.com/renatootescu/ETL-pipeline/blob/main/sparkFiles/sparkProcess.py) are intentionally verbose for a better understanding of the functionality. 

## Scenario

The Romanian COVID-19 data, provided by https://datelazi.ro/, contains COVID-19 data for each county, including the total COVID numbers from one day to the next. It does not contain the difference in numbers between the days (i.e. for county X in day 1 there were 7 cases, in day 2 there were 37 cases). This data is loaded as a json file in the s3 bucket. 

Find the differences between days for all counties (i.e. for county X there were 30 more cases in day 2 than in day 1). If the difference is smaller than 0 (e.g. because of a data recording error), then the difference for that day should be 0.

## Base concepts

 - [Data Engineering](https://realpython.com/python-data-engineer/)
 - [ETL (Extract, Transform, Load)](https://en.wikipedia.org/wiki/Extract,_transform,_load)
 - [Pipeline](https://en.wikipedia.org/wiki/Pipeline_(computing))
 - [Data Lake](https://en.wikipedia.org/wiki/Data_lake)
 - [Data Warehouse](https://en.wikipedia.org/wiki/Data_warehouse)
 - [Data Schema](https://en.wikipedia.org/wiki/Database_schema)
 - [Apache Airflow](https://airflow.apache.org/docs/apache-airflow/stable/index.html) ([wikipedia page](https://en.wikipedia.org/wiki/Apache_Airflow))
 - [Airflow DAG](https://airflow.apache.org/docs/apache-airflow/stable/concepts.html#dags)
 - [Airflow XCom](https://airflow.apache.org/docs/apache-airflow/stable/concepts.html?highlight=xcom#xcoms)
 - [Apache Spark](https://spark.apache.org/), speciffically the [PySpark](https://spark.apache.org/docs/latest/api/python/getting_started/quickstart.html) api ([wikipedia page](https://en.wikipedia.org/wiki/Apache_Spark))
 - [Amazon Web Services (AWS)](https://aws.amazon.com/) ([wikipedia page](https://en.wikipedia.org/wiki/Amazon_Web_Services))
 - [s3](https://aws.amazon.com/s3/) ([wikipedia page](https://en.wikipedia.org/wiki/Amazon_S3))
 - [Redshift](https://aws.amazon.com/redshift/?whats-new-cards.sort-by=item.additionalFields.postDateTime&whats-new-cards.sort-order=desc) ([Wikipedia page](https://en.wikipedia.org/wiki/Amazon_Redshift))
 - [mongoDB](https://www.mongodb.com/) ([wikipedia page](https://en.wikipedia.org/wiki/MongoDB))

## Prerequisites
- [Docker](https://docs.docker.com/get-docker/)
- [Docker Compose](https://docs.docker.com/compose/)
- [AWS s3 bucket](https://docs.aws.amazon.com/AmazonS3/latest/userguide/create-bucket-overview.html)
- [mongoDB database](https://www.mongodb.com/basics/create-database)

## Set-up

Download / pull the repo to your desired location.

You will have to create an AWS s3 user specifficaly for Airflow to interact with the s3 bucket.
The credentials for that user will have to be saved in the [s3 file](https://github.com/renatootescu/ETL-pipeline/blob/main/airflow-data/creds/s3) found the directory [**/airflow-data/creds**](https://github.com/renatootescu/ETL-pipeline/tree/main/airflow-data/creds):

    [airflow-spark1]
    aws_access_key_id = 
    aws_secret_access_key = 

On rows 17 and 18 in [**dags/dagRun.py**](https://github.com/renatootescu/ETL-pipeline/blob/main/dags/dagRun.py) you have the option to choose what databases system to use, mongoDB (noSQL) or Amazon Redshift (RDBMS), just by commenting/uncommenting one or the other:

    # database = 'mongoDB'
    database = 'Redshift'

If you want to use **mongoDB**, you will have to enter the mongoDB connection string (or environment variable or file with the string) in the [**dags/dagRun.py**](https://github.com/renatootescu/ETL-pipeline/blob/main/dags/dagRun.py) file, line 22:

    client = pymongo.MongoClient('mongoDB_connection_string')
    
If you want to use a **Redshift** cluster, you will have to provide your Amazon Redshift database name, host and the rest of the credentials from row 29 to 34 in [**dags/dagRun.py**](https://github.com/renatootescu/ETL-pipeline/blob/main/dags/dagRun.py):
    
    dbname = 'testairflow'
    host = '*******************************.eu-central-1.redshift.amazonaws.com'
    port = '****'
    user = '*********'
    password = '********************'
    awsIAMrole = 'arn:aws:iam::************:role/*******

You will have to change the s3 bucket name and file key (the name of the file saved in the s3 bucket) located at lines 148 and line 150 in [**dags/dagRun.py**](https://github.com/renatootescu/ETL-pipeline/blob/main/dags/dagRun.py): 

    # name of the file in the AWS s3 bucket
    key = 'countyData.json'
    # name of the AWS s3 bucket
    bucket = 'renato-airflow-raw'

In the repo directory, execute the following command that will create the .env file containig the Airflow UID and GID needed by docker-compose:

    echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env

## Installation

Start the installation with:

    docker-compose up -d

This command will pull and create Docker images and containers for Airflow, according to the instructions in the [docker-compose.yml](https://github.com/renatootescu/ETL-pipeline/blob/main/docker-compose.yml) file:

<p align="center"><img src=https://user-images.githubusercontent.com/19210522/114414670-b43ab980-9bb7-11eb-8ea8-061385b14980.gif></p>

After everything has been installed, you can check the status of your containers (if they are healthy) with:

    docker ps

**Note**: it might take up to 30 seconds for the containers to have the **healthy** flag after starting.

<p align="center"><img src=https://user-images.githubusercontent.com/19210522/114403953-ed6e2c00-9bad-11eb-9e6e-8f85d7fced2e.png></p>


## Airflow Interface

You can now access the Airflow web interface by going to http://localhost:8080/. If you have not changed them in the docker-compose.yml file, the default user is **airflow** and password is **airflow**:

<p align="center"><img src=https://user-images.githubusercontent.com/19210522/114421290-d5060d80-9bbd-11eb-842e-13a244996200.png></p>

After signing in, the Airflow home page is the DAGs list page. Here you will see all your DAGs and the Airflow example DAGs, sorted alphabetically. 

Any DAG python script saved in the directory [**dags/**](https://github.com/renatootescu/ETL-pipeline/tree/main/dags), will show up on the DAGs page (e.g. the first DAG, `analyze_json_data`, is the one built for this project).

**Note**: If you update the code in the python DAG script, the airflow DAGs page has to be refreshed

**Note**: If you do not want to see any Airflow example dags, se the `AIRFLOW__CORE__LOAD_EXAMPLES:` flag to `False` in the [docker-compose.yml](https://github.com/renatootescu/ETL-pipeline/blob/main/docker-compose.yml) file before starting the installation.

<p align="center"><img src=https://user-images.githubusercontent.com/19210522/114454069-dbf34700-9be2-11eb-8040-f57407adf856.png></p>

Click on the name of the dag to open the DAG details page:

<p align="center"><img src=https://user-images.githubusercontent.com/19210522/114457291-8882f800-9be6-11eb-9090-1f45af9f92ea.png></p>

On the Graph View page you can see the dag running through each task (`getLastProcessedDate`, `getDate`, etc) after it has been unpaused and trigerred:

<p align="center"><img src=https://user-images.githubusercontent.com/19210522/114459521-50c97f80-9be9-11eb-907a-3627a21d52dc.gif></p>


## Pipeline Task by Task

#### Task `getLastProcessedDate`

Finds the last processed date in the mongo database and saves/pushes it in an Airflow XCom

#### Task `getDate`

Grabs the data saved in the XCom and depending of the value pulled, returns the task id `parseJsonFile` or the task id `endRun`

#### Task `parseJsonFile`

The json contains unnecessary data for this case, so it needs to be parsed to extract only the daily total numbers for each county. 

If there is any new data to be processed (the date extracted in the task `getLastProcessedDate` is older than dates in the data) it is saved in a temp file in the directory [**sparkFiles**](https://github.com/renatootescu/ETL-pipeline/tree/main/sparkFiles):

<p align="center"><img src=https://user-images.githubusercontent.com/19210522/114530253-725f5100-9c53-11eb-942f-73e07baf281d.png></p>

**i.e.**: for the county AB, on the 7th of April, there were 1946 COVID cases, on the 8th of April there were 19150 cases

It also returns the task id `endRun` if there was no new data, or the task ID `processParsedData`

#### Task `processParsedData`

Executes the PySpark script [**sparkFiles/sparkProcess.py**](https://github.com/renatootescu/ETL-pipeline/blob/main/sparkFiles/sparkProcess.py). 

The parsed data is processed and the result is saved in another temporary file in the [**sparkFiles**](https://github.com/renatootescu/ETL-pipeline/tree/main/sparkFiles) directory:

<p align="center"><img src=https://user-images.githubusercontent.com/19210522/114529905-1bf21280-9c53-11eb-86e7-1f3110b155ce.png></p>

**i.e.**: for the county AB, on the 8th of April there were 104 more cases than on the 7th of April

#### Task `saveToDB`

Save the processed data either in the **mongoDB** database:

<p align="center"><img src=https://user-images.githubusercontent.com/19210522/114542796-0683e500-9c61-11eb-8bfa-be4673a47584.png></p>

Or in **Redshift**:

<p align="center"><img src=https://user-images.githubusercontent.com/19210522/115469801-f611d280-a23d-11eb-8541-e0a34530096f.png></p>

**Note**: The Redshift column names are the full name of the counties as the short version for some of them conflicts with SQL reserved words

#### Task `endRun`

Dummy task used as the end of the pipeline


## Shut Down and Restart Airflow

If you want to make changes to any of the configuration files [docker-compose.yml](https://github.com/renatootescu/ETL-pipeline/blob/main/docker-compose.yml), [Dockerfile](https://github.com/renatootescu/ETL-pipeline/blob/main/Dockerfile), [requirements.txt](https://github.com/renatootescu/ETL-pipeline/blob/main/requirements.txt) you will have to shut down the Airflow instance with:

    docker-compose down
    
This command will shut down and delete any containers created/used by Airflow.

For any changes made in the configuration files to be applied, you will have to rebuild the Airflow images with the command:

    docker-compose build

Recreate all the containers with:

    docker-compose up -d


## Learning Resources

These are some useful learning resources for anyone interested in Airflow and Spark:

 - [Apache Airflow Documentation](https://airflow.apache.org/docs/apache-airflow/stable/index.html)
 - [Spark by examples](https://sparkbyexamples.com/pyspark-tutorial/)
 - [DataScience Made Simple](https://www.datasciencemadesimple.com/pyspark-string-tutorial/)
 - [Marc Lambreti](https://marclamberti.com/)
 - [Medium](https://medium.com/@itunpredictable/apache-airflow-on-docker-for-complete-beginners-cf76cf7b2c9a)
 - [Towards Data Science](https://towardsdatascience.com/getting-started-with-airflow-using-docker-cd8b44dbff98)
 - [Precocity](https://precocityllc.com/blog/airflow-and-xcom-inter-task-communication-use-cases/)
 - [Databricks](https://databricks.com/blog/2015/07/15/introducing-window-functions-in-spark-sql.html)

## License
You can check out the full license [here](https://github.com/renatootescu/ETL-pipeline/blob/main/LICENSE)

This project is licensed under the terms of the **MIT** license.

<h1 align="center">Summary Project with ETL Pipeline</h1>

<p align="center">
    <a href="#about">About</a> •
    <a href="#subject">Subject</a> •
    <a href="#Prerequisites">Prerequisites</a> •
    <a href="#info">Info</a> •
    <a href="#etl">ETL</a> •
</p>

## about

Github에 있는 Data ETL Project를 따라하며 아래와 같은 사항을 습득하기 위함

- Cloud / Spark 환경 구성
- Acquire good coding style

## subject

<p align="center"><img src=https://user-images.githubusercontent.com/19210522/115540283-9b609100-a2a6-11eb-9f48-08f3a17528d8.png></p>

- 주제 : 나라별 / 일별 코로나 확진자 추이

- 특징 : daily data

- 에러 : if difference lower then 0 so that should be 0

## Prerequisites

- Docker
- AWS S3
- Spark
- MongoDB
- Airflow

## Code

1. Task \_ getLastProcessedDate

   DB 연결 및 마지막 수집 데이터 일자 반환(없으면 '2020-01-01')

2. Task \_ getDate

   1번 task가 일자를 반환하면 task(parseJsonFile) 아니면 task(endRun) 반환

3. Task \_ parseJsonFile

   S3에서 데이터 다운로드 & 파일을 읽고 필요한 정보만 Parsing & Parsing data 저장 & task_id(processParsedData) 반환 중간에 실패했을 경우 task_id(endRun)

4. Task \_ saveToDB

   Spark 처리한 결과 데이터(results.csv)를 읽고 mongoDB에 넣을 수 있도록 변환 후 저장(db=countyDiff) & results.csv 파일 삭제

5. Process \_ processParsedData

   Spark process

### Process `sparkProcess.py`

1. dag를 통해 SparkFiles 에 저장된 parsedData.csv를 읽는다.

2. window - lead()를 통해 다음 일자 값({country}Diff) 필드 추가

3. 일자 간 차이를 저장

## info

#### Credentials

        aws_access_key_id =
        aws_secret_access_key =

#### DB

        database = 'mongoDB'
        client = pymongo.MongoClient('mongoDB_connection_string')

#### S3

        key = 'countryData.json'
        bucket = 'plerin-arilfow-raw'

#### Airflow

        echo -e "ARIFLOW_UID=$(id -u)\nAirflow_GID=0" > .env

## etl

#### Task `getLastProcessDate` :

find last processed date in DB so return to Airflow XCom

#### Task `getDate` :

get data from XCom, return to task_id of `parseJsonFile` or `endRun`

#### Task `parseJsonFile` :

extract daily data each country among the json data because of have all data doesn't needed. if find new data and newer than last data of `getLastProcessedDate` even not return task_id of `endRun`

#### Task `processParseData `:

processed data store another temp file in sparkFile

#### Task `saveToDB` :

processed data store in DB

#### Task `endRun` :

data pipelien last dummy task

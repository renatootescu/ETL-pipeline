from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import lead, lag
import os

# the parsed data csv file
parsedData = '/opt/airflow/sparkFiles/parsedData.csv'

# start a spark session and set up its configuration
spark = SparkSession \
    .builder \
    .appName("Pysparkexample") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

# create a spark dataframe using the data in the csv
df = spark.read.csv(parsedData,
                    header='true',
                    inferSchema='true',
                    ignoreLeadingWhiteSpace=True,
                    ignoreTrailingWhiteSpace=True)

# list for columns subtractions
colDiffs = []
# get only the county columns from the df columns list
countyCols = df.columns[1:]
# change the schema/type of the dateFor column from string to date
df = df.withColumn("dateFor", F.to_date("dateFor", "yyyy-MM-dd"))
# Window function spec to partition the df and sort it by Dates descending
# The entire dataset is partitioned (no argument passed to partitionBy) as there are no dates that show multiple times.
windowSpec = Window.partitionBy().orderBy(F.col('dateFor').desc())
# for each county column in the columns list
for county in countyCols:
    # add a new column, countynameDiff, to the df containing the same numbers but shifted up by one using "lead"
    # E.g.: if a column X contains the numbers [1, 2, 3], applying the "lead" window function, with 1 as argument, will
    # shift everything up by 1 and the new XDiff column will contain [2, 3, none]
    df = df.withColumn(f'{county}Diff', lead(county, 1).over(windowSpec))
    # add the subtraction to the list with the condition that if the calculated value is lower than 0, then save 0
    # this saves the subtraction formula in the list, not the result of the subtraction.
    # the header of the subtraction result column will be the same as the "county" by applying "alias"
    colDiffs.append(F.when((df[county] - df[f'{county}Diff']) < 0, 0)
                    .otherwise(df[county] - df[f'{county}Diff']).alias(county))
# select the dateFor column and calculate the subtractions in the df, returning a new dataframe with the results
result = df.select('dateFor', *colDiffs).fillna(0)
# convert the result to a pandas dataframe and save it as a csv
# warning: the conversion is executed in memory. Other methods might be better suited for large datasets
result.toPandas().to_csv('/opt/airflow/sparkFiles/results.csv',
                         sep=',',
                         header=True,
                         index=False)

# delete the parsed data csv from the working directory
os.remove(parsedData)

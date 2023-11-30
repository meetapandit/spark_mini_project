import pandas as pd
from google.cloud import storage
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import *
import pyspark.sql.functions as F
from pyspark.sql import Window
from pyspark.sql.types import *
from pyspark.sql import SparkSession
import glob
import os

storage_client = storage.Client("mp-capstone-1")

# create a bucket object
bucket = storage_client.get_bucket('car_post_sales_report')

# if bucket does not exist then create new bucket
if bucket is None: 
    bucket = storage_client.create_bucket("car_post_sales_report")

blob = bucket.blob('input/raw_data.txt')
blob.upload_from_filename('/Users/meetapandit/DE_Bootcamp/spark_mini_project/data.csv')
print("URL of saved data to bucket:", blob.public_url)

# create spark session object
print("create spark session")
spark = SparkSession.builder\
                    .appName("read_file")\
                    .master("local[*]")\
                    .config('spark.jars.packages', 'com.google.cloud.bigdataoss:gcs-connector:hadoop2-1.9.17')\
                    .config('spark.jars.excludes',  'javax.jms:jms,com.sun.jdmk:jmxtools,com.sun.jmx:jmxri')\
                    .config('spark.driver.userClassPathFirst','true')\
                    .config('spark.executor.userClassPathFirst','true')\
                    .config('spark.hadoop.fs.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem')\
                    .config('spark.hadoop.fs.gs.auth.service.account.enable', 'true')\
                    .config("spark.hadoop.google.cloud.auth.service.account.email", "382306615898-compute@developer.gserviceaccount.com")\
                    .config("spark.hadoop.google.cloud.service.account.json.keyfile", "/Users/meetapandit/DE_Bootcamp/Capstone_1/AlphaAdvantage_stockdata/my-credentials.json")\
                    .getOrCreate()

# define schema
print("define schema")
schema = StructType([StructField('incident_id', IntegerType(), True),
                     StructField('incident_type', StringType(), True),
                     StructField('vin_number', StringType(), True),
                     StructField('make', StringType(), True),
                     StructField('model', StringType(), True),
                     StructField('year', StringType(), True),
                     StructField('incident_date', DateType(), True),
                     StructField('description', StringType(), True)
])
# read data from cloud storage bucket
print("read data from google cloud storage")
post_sales_df = spark.read.csv('gs://car_post_sales_report/input/raw_data.txt', schema = schema)

post_sales_df.show()

win_make = Window.partitionBy('vin_number').orderBy(desc('make'))
win_model = Window.partitionBy('vin_number').orderBy(desc('model'))
win_year = Window.partitionBy('vin_number').orderBy(desc('year'))
post_sales_group = post_sales_df.withColumn('new_make', F.last('make',True).over(win_make))\
                                .withColumn('new_model', F.last('model',True).over(win_model))\
                                .withColumn('new_year', F.last('year',True).over(win_year))\
                                .drop('year')
post_sales_group.show()

# filter out all records other than accident
post_sales_filtered = post_sales_group.filter(post_sales_group.incident_type == 'A')\
                                      .groupBy('new_make', 'new_year')\
                                      .agg(count("*").alias('count_acc'))
post_sales_filtered.show()

# write the output file to google cloud bucket
post_sales_filtered.write.mode("overwrite").parquet("gs://car_post_sales_report/output")




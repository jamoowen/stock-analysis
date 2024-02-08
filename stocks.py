import pyspark.sql.functions as psf
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import time
import requests
import pandas as pandas
from dotenv import load_dotenv
import os
from helpers import request_prices_polygon, round_cols, compare_share_prices
from pathlib import Path

# this is the main file which should be run to produce outputs
# the function which makes the api requests and is found within helpers.py, along with the function to compare prices

spark = SparkSession.builder.getOrCreate()
# set spark write to dynamic so that we can append data without worrying about adding duplicates through a rerun
spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
# Load environment variables from .env file
load_dotenv()
# Access the API key
api_key = os.getenv("API_KEY")

print("Beginning stock comparison process")
# Read in csv and replace FB and ANTM tickers (they have changed)
df_csv = spark.read.csv("./input_data/stocks.csv", header=True, sep=',') \
    .withColumn('symbol', psf.regexp_replace('symbol', 'FB', 'META')) \
    .withColumn('symbol', psf.regexp_replace('symbol', 'ANTM', 'ELV')) \
    .withColumn('initial_investment', psf.lit(10000))

# convert back to a python dict to make api requests easier
pandas_df_csv = df_csv.toPandas()
stocks_dict = pandas_df_csv.to_dict(orient='records')

# set dates (furthest date we can request is 2 years back) 
current_date = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
old_date = (datetime.today() - relativedelta(years=2)).strftime('%Y-%m-%d')

select_expression = ["company_name", "symbol", "cast(initial_investment as double) as initial_investment", "CAST(date as date) as date", "CAST(price as double) as price"]

# if output path exists, check if the dates are the latest possible
if (Path.cwd() / 'outputs' / 'polygon_stock_data.parquet').exists():
    print("Found previous output file")
    df_polygon_data = spark.read.parquet('./outputs/polygon_stock_data.parquet')
    max_date = df_polygon_data.select(psf.max("date")).collect()[0][0]
    min_date = df_polygon_data.select(psf.min("date")).collect()[0][0]

    # if there is new data available, pull from polygon
    if datetime.strptime(current_date, '%Y-%m-%d').date() > max_date:
        print("New data available, requesting now...")
        dict_new = request_prices_polygon(stocks_dict=stocks_dict, date_period=current_date, api_key=api_key)
        df_new = spark.createDataFrame(dict_new).selectExpr(select_expression)
        df_new.write.mode('overwrite').partitionBy('date').format('parquet').save('./outputs/polygon_stock_data.parquet')
    
    # if there is older data available, pull 
    if datetime.strptime(old_date, '%Y-%m-%d').date() < min_date:
        print("Older data available, requesting now...")
        dict_old = request_prices_polygon(stocks_dict=stocks_dict, date_period=old_date, api_key=api_key)
        df_old = spark.createDataFrame(dict_old).selectExpr(select_expression)
        df_old.write.mode('overwrite').partitionBy('date').format('parquet').save('./outputs/polygon_stock_data.parquet')
    
    df = spark.read.parquet('./outputs/polygon_stock_data.parquet')
else:
    print("No output data found. Pulling oldest and newest available share prices...")
   # since polygon free tier is only 5 requests per min, this method takes 40 mins to run
    dict_old = request_prices_polygon(stocks_dict=stocks_dict, date_period=old_date, api_key=api_key)
    df_old = spark.createDataFrame(dict_old)

    dict_new = request_prices_polygon(stocks_dict=stocks_dict, date_period=current_date, api_key=api_key)
    df_new = spark.createDataFrame(dict_new)
    
    df = df_new.union(df_old)
    df = df.selectExpr(select_expression)
    df.write.mode('overwrite').partitionBy('date').format('parquet').save('./outputs/polygon_stock_data.parquet')

duplicates = df.groupBy(['date', 'company_name']).count().filter('count>1').count()
assert (duplicates == 0, "There are duplicates in output!")
# compare the relative investments and write outputs (answers)
df_compare = compare_share_prices(df, current_date, old_date)
df_compare.show()

print("\nProcessing complete.")
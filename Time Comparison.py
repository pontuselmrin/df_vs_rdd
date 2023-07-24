from pyspark.sql import SparkSession, Functions as F
from time import time
import pyspark.pandas as ps
import pandas as pd

"""
Simple outline script to calculate the difference in computation time for RDDs vs dataframes.
The purpose is to confirm that in certain cases, RDDs allow for more efficient function calls
"""


# Function to calculate average cost of borrowing using DataFrames
def calculate_average_dataframe(data_df):
    tick = time()
    total_sum = data_df.agg(F.sum("OBS_VALUE")).collect()[0][0]
    total_rows = data_df.count()
    avg_cost_of_borrowing = total_sum / total_rows
    tock = time()
    return avg_cost_of_borrowing, tock - tick

# Function to calculate average cost of borrowing using RDDs
def calculate_average_rdd(data_rdd):
    tick = time()
    total_sum = data_rdd.map(lambda col: float(col["OBS_VALUE"])).sum()
    total_rows = data_rdd.count()
    avg_cost_of_borrowing = total_sum / total_rows
    tock = time()
    return avg_cost_of_borrowing, tock - tick

# Start local SparkSession
spark = SparkSession.builder.master("local[1]").appName("time_test").getOrCreate()

# Fetching data on cost of borrowing for households for house purchases
api_url = 'https://data-api.ecb.europa.eu/service/data/MIR/M.U2.B.A2C.AM.R.A.2250.EUR.N?format=csvdata'

# Spark Session setup

try:
    # Fetching ECB data
    housing_data = pd.read_csv(api_url)
    spark_ps_df = ps.from_pandas(housing_data)

    # Convert the data to DataFrame and RDD
    spark_df = spark_ps_df.to_spark()
    spark_rdd = spark_df.rdd

    # Calculate average exchange rate using DataFrames
    avg_df, time_df = calculate_average_dataframe(spark_df)
    print(f"Average cost of borrowing using DataFrames: {avg_df}")
    print(f"Time taken using DataFrames: {time_df} seconds")

    # Calculate average exchange rate using RDDs
    avg_rdd, time_rdd = calculate_average_rdd(spark_rdd)
    print(f"Average cost of borrowing using RDDs: {avg_rdd}")
    print(f"Time taken using RDDs: {time_rdd} seconds")

    methods = {time_rdd : "RDD", time_df : "DataFrame"}
    fastest = min(time_df,time_rdd)
    slowest = max(time_df,time_rdd)

    print(f"The fastest method was {methods[fastest]}, 
          which was {round((1-fastest/slowest)*100,2)}% faster than {methods[slowest]}")


finally:
    # Stop the Spark Session
    spark.stop()
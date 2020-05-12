# from pyspark.sql import SparkSession
import os
import pandas as pd
from datetime import date
# from pyspark.sql.types import *
#
# spark = SparkSession.builder.appName("123").getOrCreate()
#
# schema = StructType([
#     StructField("col1", StringType(), True),
#     StructField("col2", StringType(), True),
#     StructField("col3", StringType(), True),
#     StructField("col4", StringType(), True),
#     StructField("col5", StringType(), True),
# ])
#
# path = '/Users/ngongocnam/Desktop/data_telco_vina/pending_schema_data/angiang/*.csv'
#
# df = spark.read.load(path=path, format="csv", schema=schema)
# df.show()
path = '/Users/ngongocnam/Desktop/data_telco_vt050.xlsx'

df = pd.read_excel(path)
for col in list(df.columns):
    print(col)
    if str(col).startswith("Unnamed"):
        df.__delitem__(col)
print(df.columns)
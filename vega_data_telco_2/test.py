from pyspark import SparkSession

spark = SparkSession.builder.appName("123").getOrCreate()
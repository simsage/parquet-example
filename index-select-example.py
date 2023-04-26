#!/usr/bin/python3

import pyspark
from pyspark.sql import SparkSession

# python3 -m pip install pyspark

spark = SparkSession.builder.appName("query-test").getOrCreate()
parDF1 = spark.read.parquet("index-demo_knowledge_base-summary-2023-1-16.parquet")

parDF1.createOrReplaceTempView("parquetTable")

# this prints out the schema (i.e. the fields) of the above table
parDF1.printSchema()

query1 = spark.sql("select * from ParquetTable")
query1.show(truncate=False)

# example: get all emails from the set where they have values
# query2 = spark.sql("select * from ParquetTable where is_entity = true and word = 'email'")
# query2.show(truncate=False)


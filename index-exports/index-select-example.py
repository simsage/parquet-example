#!/usr/bin/env python3

#
# open and query an INVENTORY INDEX export parquet file - this file is more for language
# and content analysis of indexes.  This file is intended for SimSage internal use.
# The kind of analysis is useful for content and index analysis by the SimSage team.
#


import pyspark
from pyspark.sql import SparkSession
import sys

if len(sys.argv) != 2:
    print("takes one parameter: /path/to/file.parquet")
    exit(1)
file_name = sys.argv[1]

# python3 -m pip install pyspark

spark = SparkSession.builder.appName("query-test").getOrCreate()
parDF1 = spark.read.parquet(file_name)

parDF1.createOrReplaceTempView("parquetTable")

# this prints out the schema (i.e. the fields) of the above table
parDF1.printSchema()

query1 = spark.sql("select * from ParquetTable")
query1.show(truncate=False)

# example: get all emails from the set where they have values
# query2 = spark.sql("select * from ParquetTable where is_entity = true and word = 'email'")
# query2.show(truncate=False)


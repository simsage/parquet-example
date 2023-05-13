#!/usr/bin/env python3

# python3 -m pip install pyspark

import pyspark
from pyspark.sql import SparkSession
import sys

if len(sys.argv) != 2:
    print("takes one parameter: /path/to/file.parquet")
    exit(1)
file_name = sys.argv[1]

spark = SparkSession.builder.appName("query-test").getOrCreate()
# this is the document-inventory parquet file
parDF1 = spark.read.parquet(file_name)
parDF1.createOrReplaceTempView("parquetTable")

# this prints out the schema (i.e. the fields) of the above parquet file
# there are a lot of "extra metadata" fields in this table that are automatically created and added - so it might look messy - but it is done so you can
# select on any metadata item
parDF1.printSchema()

# get the SimSage id, full_path (remote location), SimSage source name, the size of the data, a list of very similar items (set on source as a threshold), and identical items
query2 = spark.sql("select id, full_path, source, size, acls, very_similar, identical from ParquetTable order by id")
number_of_rows_to_show = 10_000
query2.show(number_of_rows_to_show, truncate=False)

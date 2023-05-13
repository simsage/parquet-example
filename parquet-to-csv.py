#!/usr/bin/env python3

# python3 -m pip install pyspark

# convert a parquet file to a CSV file with header

import pyspark
from pyspark.sql import SparkSession
import sys

if len(sys.argv) != 3:
    print("convert a parquet file to a csv file (can output multiple csv-parts)")
    print("takes two parameter: /path/to/file.parquet /write/to/new/folder/")
    exit(1)
in_file_name = sys.argv[1]
out_file_name = sys.argv[2]

spark = SparkSession.builder.appName("query-test").getOrCreate()
df = spark.read.parquet(in_file_name)
df.createOrReplaceTempView("parquetTable")
df.write.option("header", True).csv(out_file_name)

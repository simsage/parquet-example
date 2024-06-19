#!/usr/bin/env python3

#
# this script converts a spark parquet file to a series of CSV files (depending on size it splits)
#
# 1. use SimSage to generate a DOCUMENT INVENTORY parquet file from your admin UX in the `inventory` section
# 2. wait for this file to generate, then download it to your own environment
# 3. use this `parquet-to-csv.py` file to your parquet file to a series of CSV files
#

# convert a parquet file to a CSV file with header

import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
import sys
import os, shutil
from utils import create_reports

if len(sys.argv) != 3:
    print("generate CSV reports from a parquet file")
    print("takes two parameters: customer_name /path/to/file.parquet")
    print("                      the customer_name is also a folder temporary  name")
    exit(1)

in_file_name = sys.argv[2]
out_file_path = sys.argv[1]
customer = out_file_path.split('/')[-1]
customer = customer.split('\\')[-1]

spark = SparkSession.builder.appName("query-test").config("spark.sql.parquet.columnarReaderBatchSize", "256").getOrCreate()
spark.conf.set("spark.sql.debug.maxToStringFields", 1000)

df = spark.read.parquet(in_file_name)
df.createOrReplaceTempView("parquetTable")
df.write.option("header", True).csv(out_file_path)

# if we had success, rename the files in the output folder (customer name) to 1,2,3.. csv
success_file = "{}/_SUCCESS".format(out_file_path)
if os.path.exists(success_file):
    print("found {}, copying CSV data into {}.csv".format(success_file, customer))
    csv_files = [os.path.join(out_file_path, f) for f in os.listdir(out_file_path) if os.path.isfile(os.path.join(out_file_path, f)) and f.startswith("part-") and f.endswith(".csv")]
    csv_files.sort()
    with open("{}.csv".format(customer), "wt") as writer:
        for file in csv_files:
            print("copying {}".format(file))
            with open(file, "rt") as reader:
                for line in reader:
                    writer.write(line)
    print("created {}.csv".format(customer))
    if os.path.isdir(out_file_path):
        print("removing temp folder: {}".format(out_file_path))
        shutil.rmtree(out_file_path)

    print("creating reports for {}".format(customer))
    create_reports("{}.csv".format(customer))

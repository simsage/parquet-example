## pySpark
This how to use pySpark with SimSage exports.  How to read a SimSage parquet file using python.
This project is written for python 3, any modern version of python 3 should work.

Parquet is a file format for big data.  Apache Spark is a powerful technology that puts a complete SQL-99 compatible interface around a single parquet file.

using a virtual environment and the `requirements.txt` file.

```bash
virtualenv venv
source venv/bin/activate
pip install -r requirements.txt
```

## convert a Parquet file to a CSV file
Run `generate_reports.py` to convert the entire contents of your parquet file to a `csv` file with reporting.
This takes two parameters, a `customer_name` that is used for the name of the reports and a temporary folder for writing into,
and the `/path/to/parquet-file.parquet` location of your parquet file.

```bash
# to convert your parquet file at /path/to/file.parquet
python3 generate_reports.py customer /path/to/file.parquet

# using the data included
python3 generate_reports.py demo index-demo_knowledge_base-summary-2023-1-16.parquet
```

#### The files

| filename                      | description                                                                           |
|-------------------------------|---------------------------------------------------------------------------------------|
| generate_reports.py           | Generate a series of CSV files from a parquet file (see below)                        |
| utils.py                      | Helper utilities for generate_reports.py                                              |
| query_example.py              | A sample SQL 99 query on a parquet file                                               |
| upload_parquet_to_postgres.py | use Pandas and PsyCopg2 to upload data from a parquet file into a Postgres SQL server |


#### The reports

The report files are as follows (here `customer` is the name you used above for generating these files)

|filename                        | description                                                        |
|--------------------------------|--------------------------------------------------------------------|
|customer.csv                    | the raw csv output of the parquet file                             |
|customer-path_report.csv        | the different paths found inside all the files                     |
|customer-pii_report.csv         | Personally Identifiable Information inside your files              |
|customer-sec_report.csv         | Security / ACLs grouped for each file                              |
|customer-similarity_report.csv  | Similar and identical files grouped                                |
|customer-type_report_1.csv      | Different file types with sizes, counts, and oldest / newest dates |


## Spark Raw Query

```
## run the SQL-99 query example
Simple, run `query_example.py` to get a view of what is inside a SimSage parquet document export.

# to view part of your parquet file at /path/to/file.parquet
python3 query_example.py /path/to/file.parquet
```

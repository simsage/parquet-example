## pySpark demo
This is a pySpark demo, showing how to read a SimSage parquet file using python.
This project is written for python 3, at the time of writing python 3.11, however any modern version of python 3 should work.

Parquet is a file format for big data.  Apache Spark is a powerful technology that puts a complete SQL-99 compatible interface around a single parquet file.

## set up
Create a virtual-environment (good practise but not required) or a folder for your project.
Then install pySpark like so (using python 3) inside this project folder.

pySpark requires Java to be installed.  We recommend you install the free OpenJDK (or OpenJRE)

```bash
# make sure your system is up-to-date
sudo apt update
sudo apt upgrade -yqq

# install the open-jdk
sudo apt install openjdk-17-jdk

# verify installation
java --version
```

#### this section is optional

install `pySpark`
```bash
python3 -m pip install pyspark
```

or using a virtual environment

```bash
virtualenv venv
source venv/bin/activate
pip install -r requirements.txt
```

## converting your parquet file to a csv file
run `generate_reports.py` to convert the entire contents of your parquet file to a `csv` file with reporting.
This takes two parameters, a `customer_name` that is used for the name of the reports and a temporary folder for writing into,
and the `/path/to/parquet-file.parquet` location of your parquet file.

```bash
# to convert your parquet file at /path/to/file.parquet
python3 generate_reports.py customer /path/to/file.parquet

# using the data included
python3 generate_reports.py demo index-demo_knowledge_base-summary-2023-1-16.parquet
```

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

## The schema
Exported records contain all metadata found.  As such, we cannot predict what metadata is in a record, or what a lot
of these metadata items mean.

However, we have a set of well known values defined by SimSage.  These are:

* id: long (nullable = true)
the SimSage ID, a unique number between 1 and 1,000,000,000 for this record.

* full_path: string (nullable = true)
The full path of the record including the filename.  A URL for a web record, a file-path with filename for a file-type record.

* filename: string (nullable = true)
the filename part of the path only

* source: string (nullable = true)
the name of the SimSage source this record is associated with.

* title: string (nullable = true)
The title of the record.  For document types, the title from its metadata.  For some source-types this can be mapped inside SimSage (e.g. database records)

* author: string (nullable = true)
The author of the record.  For document types, the author/creator from its metadata.  For some source-types this can be mapped inside SimSage (e.g. database records)

* type: string (nullable = true)
The mime-type of this record, e.g. application/pdf

* extension: string (nullable = true)
The file-extension of this record, e.g. pdf

* inventory: string (nullable = true)
A string indicating if this record was put in SimSage's inventory (i.e. not processed) or not.  'true' if it is in inventory, and 'false' if not.

* created: long (nullable = true)
An epoch timestamp for when this record was created.  This is a long (64 bit) value counting the milliseconds since 1970-01-01 00:00:00.000.

* last_modified: long (nullable = true)
An epoch timestamp for when this record was last-modified.  This is a long (64 bit) value counting the milliseconds since 1970-01-01 00:00:00.000.

* content_hash: string (nullable = true)
The hexadecimal string value of the md5 hash of the content of this record.

* size: long (nullable = true)
The size in bytes of this record.

* acls: string (nullable = true)
A list of email addresses or group names with RMDW (Read Modify Delete Write) indicators for access, (e.g. "Users:R,rock@simsage25.onmicrosoft.com:RWDM")

* language: string (nullable = true)
The language code for this record, indicating what language this record's content is in (e.g. "en" for English, "de" for German).

* very_similar: string (nullable = true)
A list of very similar record IDs with probabilities of similarity (e.g. "21725:0.99,21730:0.98")

* identical: string (nullable = true)

* error: string (nullable = true)
If set, the error associated with the record and why it couldn't be processed.

* ssn_count: long (nullable = true)
The number/count of US social security numbers found in this record. (example NIN number: "555-50-1234")

* nin_count: long (nullable = true)
The number/count of UK national insurance numbers found in this record (example NIN number: "QQ 123456 B")

* continent_count: long (nullable = true)
The number of contintents found in this record (example continent: "Africa")

* credit_card_count: long (nullable = true)
The number of credit card numbers found in this record (example credit-card number: "4222222222222")

* law_firm_count: long (nullable = true)
The number of Law firms found in this record.  This number depends on the law firm database set up inside SimSage and is relatively small at present.

* country_count: long (nullable = true)
The number of countries found in this record (example country: "Namibia")

* mac_address_count: long (nullable = true)
The number of mac-addresses found in this record (example mac-address: "00:1A:2B:3C:4D:5E")

* company_count: long (nullable = true)
The number of companies found in this record.  This number depends on the company database set up inside SimSage and is relatively small at present.

* money_count: long (nullable = true)
The number of monetary amounts (pounds, euros, dollars) found inside this record (example monetary amount: "$1.25")

* time_count: long (nullable = true)
The number of time values found inside this record (example time value: "11:23 pm" or "23:30:00")

* zip_count: long (nullable = true)
The number of US ZIP codes found inside this record (example zip code: "90210")

* vat_count: long (nullable = true)
The number of UK VAT numbers found inside this record (example VAT number: "GB123456789")

* phone_count: long (nullable = true)
The number of UK/US phone numbers found inside this record (example phone number: "020 7123 4567")

* city_count: long (nullable = true)
The number of cities found inside this record.  This number depends on the SimSage database set up inside SimSage.  This includes mostly the largest cities on Earth, and a large number of cities from New Zealand, the UK, and the US.

* secret_count: long (nullable = true)
The number of Truffle Hog expressions found inside this record.  Truff Hof is a set of well known patterns for API keys such as AWS keys, Facebook API Keys etc.  We scan for about thirty patterns of this database (https://github.com/trufflesecurity/trufflehog).  (Example AWS API key "apikey1234abcdefghij0123456789")

* url_count: long (nullable = true)
The number of URLs / URIs found inside this record.  This includes HTTP, HTTPS, and www. kinds of references with paths.  (Example URL: "https://simsage.co.uk/videos/")

* ip_address_count: long (nullable = true)
The number of IPv6 and/or IPv4 addresses found inside this record.  (Example IPv4 Address: "130.216.1.1")

* capital_count: long (nullable = true)
The number of capitals found inside this record.  (Example capital: "Wellington")

* postcode_count: long (nullable = true)
The number of UK postcodes found inside this record.  (Example UK postcode: "CB4 1DH")

* person_count: long (nullable = true)
The number of people's names (first names and surnames, or combined) found inside this record.  This is based on a rather large database set up inside SimSage of common first- and surnames of people.  This database is usually country specific and currently reflects a mixture of common names from the US, and UK.

* percent_count: long (nullable = true)
The number of percentage amounts found inside this record.  (Example percentages: "1%, 22.5%")

* continent: string (nullable = true)
The top 10 list of continents found inside this record, if applicable.

* law_firm: string (nullable = true)
The top 10 list of law-firms found inside this record, if applicable.

* country: string (nullable = true)
The top 10 list of countries found inside this record, if applicable.

* company: string (nullable = true)
The top 10 list of companies found inside this record, if applicable.

* money: string (nullable = true)
The top 10 list of monetary amounts found inside this record, if applicable.

* time: string (nullable = true)
The top 10 list of time-values found inside this record, if applicable.

* phone: string (nullable = true)
The top 10 list of phone-numbers found inside this record, if applicable.

* city: string (nullable = true)
The top 10 list of cities found inside this record, if applicable.

* url: string (nullable = true)
The top 10 list of URLs/URIs found inside this record, if applicable.

* capital: string (nullable = true)
The top 10 list of capitals found inside this record, if applicable.

* percent: string (nullable = true)
The top 10 list of percentages found inside this record, if applicable.

## pySpark demo
This is a pySpark demo, showing how to read a SimSage parquet file using python.
This project is written for python 3, at the time of writing python 3.11, however any modern version of python 3 should work.

Parquet is a file format for big data.  Apache Spark is a powerful technology that puts a complete SQL-99 compatible interface around a single parquet file.

## set up
Create a virtual-environment (good practise but not required) or a folder for your project.
Then install pySpark like so (using python 3) inside this project folder.

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
run `parquet-to-csv.py` to convert the entire contents of your parquet file to a `csv` file with header.
This takes an `output folder` where the utility will create one or more `csv` files depending on the size
of your parquet file.

```bash
# to convert your parquet file at /path/to/file.parquet
python3 parquet-to-csv.py /path/to/file.parquet /path/to/output/folder/

# using the data included
python3 parquet-to-csv.py index-demo_knowledge_base-summary-2023-1-16.parquet out/
```

## generate content analysis reports from the CSV file
Make sure you put all CSV files into one large one.

```bash
# for instance, change into the `out` folder where the CSV files were generated
cd out

# find the parts of the CSV file, and cat them all into one file
cat part-00000-01ed070f-b1d0-4d5e-a3c1-7825ffc21e40-c*.csv > my-file.csv

# go back into the root of this repository afterwards
cd ..
```

Now you're ready to run the csv report generator

```bash
# generate the reports, this might take some time depending on size
python3 document-csv-to-reports.py out/my-file.csv

# the resulting reports are written into the `out` folder as
# my-file-pii_report.csv, my-file-similarity_report.csv, etc.
```

## run the document-select SQL-99 query example
Simple, run `document-select-example.py` to get a view of what is inside a SimSage parquet document export.

```bash
# to view part of your parquet file at /path/to/file.parquet
python3 document-select-example.py /path/to/file.parquet
```

## example output

|id |full_path                                                                             |source     |size in bytes |acls|very_similar ids|identical ids                      |
|---|--------------------------------------------------------------------------------------|-----------|------|----|------------|-----------------------------------------------|
|64 |https://simsage.ai/                                                                   |simsage web|21304 |    |            |                                               |
|65 |https://simsage.ai/_assets/svg/footer-logo-agritech.svg                               |simsage web|15728 |    |            |                                               |
|66 |https://simsage.ai/_assets/svg/footer-logo-eu.svg                                     |simsage web|863006|    |            |                                               |
|67 |https://simsage.ai/_assets/img/graphics/g-cloud-logo.webp                             |simsage web|43116 |    |            |                                               |
|68 |https://simsage.ai/_assets/img/graphics/cyber-essentials-logo.png                     |simsage web|43311 |    |            |                                               |
|69 |https://simsage.ai/_assets/img/graphics/iso-27001-logo.jpg                            |simsage web|48585 |    |            |                                               |
|70 |https://simsage.ai/_assets/svg/logo-full.svg                                          |simsage web|4578  |    |            |                                               |
|71 |https://simsage.ai/_assets/img/graphics/why-4.png                                     |simsage web|1473  |    |            |                                               |
|72 |https://simsage.ai/_assets/img/graphics/why-3.png                                     |simsage web|5468  |    |            |                                               |
|73 |https://simsage.ai/_assets/img/graphics/why-2.png                                     |simsage web|1407  |    |            |                                               |
|74 |https://simsage.ai/_assets/img/graphics/why-1.png                                     |simsage web|5357  |    |            |                                               |
|75 |https://simsage.ai/_assets/svg/graphics/uc-find-send-100.jpg                          |simsage web|148264|    |            |98,76,77,78,79,80,81,82,83,88,89,90,91,92,93,94|
|76 |https://simsage.ai/_assets/svg/graphics/uc-improve-service-100.jpg                    |simsage web|86241 |    |            |98,75,77,78,79,80,81,82,83,88,89,90,91,92,93,94|
|77 |https://simsage.ai/_assets/svg/graphics/uc-quickly-and-easily-100.jpg                 |simsage web|102602|    |            |98,75,76,78,79,80,81,82,83,88,89,90,91,92,93,94|


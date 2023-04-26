## pySpark demo
This is a pySpark demo, showing how to read a SimSage parquet file using python.
This project is written for python 3, at the time of writing python 3.11, however any modern version of python 3 should work.

Parquet is a file format for big data.  Apache Spark is a powerful technology that puts a complete SQL-99 compatible interface around a single parquet file.

## set up
Create a virtual-environment (good practise but not required) or a folder for your project.
Then install pySpark like so (using python 3) inside this project folder.

this section is optional
```bash
# optional: create a virtual environment called 'venv' in linux to isolate the installation and packages
virtualenv -p python3 venv
# then activate the virtual environment
source venv/bin/activate
```

install `pySpark`
```bash
python3 -m pip install pyspark
```

## run the sample code
Simple, run `document-select-example.py` to get a view of what is inside a SimSage parquet document export.

```bash
python3 document-select-example.py
```

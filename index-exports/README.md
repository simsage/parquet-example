## pySpark demo

open and query an INVENTORY INDEX export parquet file - this file is more for language
and content analysis of indexes.  This file is intended for SimSage internal use.
The kind of analysis is useful for content and index analysis by the SimSage team.

1. use SimSage to generate a DOCUMENT INVENTORY parquet file from your admin UX in the `inventory` section
2. wait for this file to generate, then download it to your own environment
3. use `parquet-to-csv.py` to convert this parquet file to a series of CSV files (depending on size they split)
4. if there are more than one CSV file, "glue" them together again by appending them to each other
5. then use this utility to read through the resulting CSV file and generate the reports these reports are written to the same directory/folder the CSV is located in


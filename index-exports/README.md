## pySpark demo

open and query an INVENTORY INDEX export parquet file - this file is more for language
and content analysis of indexes.  This file is intended for SimSage internal use.
The kind of analysis is useful for content and index analysis by the SimSage team.

1. use SimSage to generate a DOCUMENT INVENTORY parquet file from your admin UX in the `inventory` section
2. wait for this file to generate, then download it to your own environment
3. run the `index-select-example.py` to output the information contained in the parquet file.

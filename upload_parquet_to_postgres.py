import pandas as pd
import psycopg2
from psycopg2 import sql

def get_postgres_type(pandas_type):
    """Maps pandas dtype to PostgreSQL data type."""
    if "int64" in str(pandas_type):
        return "BIGINT"
    elif "int32" in str(pandas_type):
        return "INTEGER"
    elif "float64" in str(pandas_type):
        return "DOUBLE PRECISION"
    elif "float32" in str(pandas_type):
        return "REAL"
    elif "object" in str(pandas_type):
        return "TEXT"
    elif "bool" in str(pandas_type):
        return "BOOLEAN"
    elif "datetime64" in str(pandas_type):
        return "TIMESTAMP"
    else:
        return "TEXT" # Default for other types

def parquet_to_postgres(parquet_file_path, db_params, schema_name, table_name):
    """
    Reads a Parquet file, creates a schema and table in PostgreSQL,
    and inserts the data.

    Args:
        parquet_file_path (str): The path to the Parquet file.
        db_params (dict): A dictionary with database connection parameters.
        schema_name (str): The name of the schema to create.
        table_name (str): The name of the table to create.
    """
    try:
        # Step 1: Read Parquet file into a pandas DataFrame
        print(f"Reading Parquet file from: {parquet_file_path}")
        df = pd.read_parquet(parquet_file_path)
        print("Parquet file read successfully.")
        print("\nDataFrame Info:")
        df.info()

        # Step 2: Connect to the PostgreSQL database
        print("\nConnecting to the PostgreSQL database...")
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        print("Database connection successful.")

        # # Step 3: Create the schema if it doesn't exist
        # print(f"Creating schema '{schema_name}' if it doesn't exist...")
        # create_schema_query = sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
        #     sql.Identifier(schema_name)
        # )
        # cursor.execute(create_schema_query)
        # print(f"Schema '{schema_name}' is ready.")
        #

        # Step 4: Dynamically generate the CREATE TABLE statement
        print(f"Generating CREATE TABLE statement for '{table_name}'...")
        columns_with_types = []
        for column_name, dtype in df.dtypes.items():
            pg_type = get_postgres_type(dtype)
            columns_with_types.append(sql.SQL("{} {}").format(
                sql.Identifier(column_name),
                sql.SQL(pg_type)
            ))

        # Drop table if it exists to start fresh
        drop_table_query = sql.SQL("DROP TABLE IF EXISTS {}").format(
            sql.Identifier(table_name)
        )
        cursor.execute(drop_table_query)
        print(f"Dropped table '{table_name}' if it existed.")

        create_table_query = sql.SQL("CREATE TABLE {} ({})").format(
            sql.Identifier(table_name),
            sql.SQL(", ").join(columns_with_types)
        )
        cursor.execute(create_table_query)
        print(f"Table '{table_name}' created successfully.")

        # Step 5: Insert data from the DataFrame into the table
        print(f"Inserting data into '{table_name}'...")
        
        # Prepare the INSERT statement
        insert_query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
            sql.Identifier(table_name),
            sql.SQL(", ").join(map(sql.Identifier, df.columns)),
            sql.SQL(", ").join(sql.Placeholder() * len(df.columns))
        )

        # Convert DataFrame to list of tuples for insertion
        df_first_1000 = df.head(1000)
        data_to_insert = [tuple(x) for x in df_first_1000.to_numpy()]

        # Execute the insert statement for all rows
        cursor.executemany(insert_query, data_to_insert)
        print(f"{len(data_to_insert)} rows inserted successfully.")

        # Commit the transaction
        conn.commit()
        print("Transaction committed.")

    except (Exception, psycopg2.Error) as error:
        print(f"Error while connecting to PostgreSQL or processing file: {error}")
        if conn:
            conn.rollback()
            print("Transaction rolled back.")
    finally:
        # Closing database connection.
        if conn:
            cursor.close()
            conn.close()
            print("PostgreSQL connection is closed.")


if __name__ == "__main__":
    # --- Configuration ---
    
    # Path to your Parquet file
    # On your local machine, this could be e.g., "C:/Users/YourUser/Downloads/data.parquet"
    # Make sure to create a dummy parquet file for testing.
    PARQUET_PATH = "sjic_knowledge_base-summary-2024-6-19.parquet"
    
    # PostgreSQL connection details
    # IMPORTANT: Replace with your actual database credentials.
    DB_CONNECTION_PARAMS = {
        "host": "localhost",
        "database": "rock",
        "user": "rock",
        "password": "Password1"
    }

    # Desired schema and table name in PostgreSQL
    SCHEMA = "public"
    TABLE = "parquet_data"

    # --- Run the main function ---
    parquet_to_postgres(PARQUET_PATH, DB_CONNECTION_PARAMS, SCHEMA, TABLE)


import os
import pandas as pd
import mysql.connector


def infer_data_types(csv_filename):
    # Read the CSV file into a Pandas DataFrame
    df = pd.read_csv(
        csv_filename, nrows=100
    )  # Limiting to first 100 rows for efficiency
    # Infer data types for each column
    data_types = {col: "VARCHAR(255)" for col in df.columns}  # Default to VARCHAR(255)
    for col in df.columns:
        dtype = df[col].dtype
        if pd.api.types.is_integer_dtype(dtype):
            data_types[col] = "INT"
        elif pd.api.types.is_float_dtype(dtype):
            data_types[col] = "DOUBLE"
        # Add more conditions as needed for other data types (e.g., date, boolean, etc.)
    return data_types


def create_table(table_name, column_data_types, database_connection):
    cursor = database_connection.cursor()
    create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ("
    for column_name, column_data_type in column_data_types.items():
        create_table_sql += f"{column_name} {column_data_type}, "
    create_table_sql = (
        create_table_sql[:-2] + ")"
    )  # Remove the trailing comma and space
    cursor.execute(create_table_sql)
    database_connection.commit()


def import_data(csv_filename, table_name, database_connection):
    cursor = database_connection.cursor()

    # Enable local infile loading for the current session
    cursor.execute("SET GLOBAL local_infile = 1")

    # Execute the LOAD DATA LOCAL INFILE command
    import_data_sql = f"LOAD DATA  INFILE %s INTO TABLE {table_name} FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 ROWS"
    with open(csv_filename, "r") as csvfile:
        cursor.execute(import_data_sql, (csv_filename,))

    # Commit changes
    database_connection.commit()


# MySQL connection configuration
mysql_config = {
    "user": "root",
    "password": "......",
    "host": "localhost",
    "database": "transfermarkt",
    "allow_local_infile": True,
}

# Connect to MySQL
connection = mysql.connector.connect(**mysql_config)

# Get list of CSV files in the current directory
csv_files = [filename for filename in os.listdir(".") if filename.endswith(".csv")]

# Loop through each CSV file
for csv_filename in csv_files:
    table_name = os.path.splitext(csv_filename)[
        0
    ]  # Use filename as table name (without extension)
    # Infer data types for columns
    column_data_types = infer_data_types(csv_filename)
    # Create table in MySQL
    create_table(table_name, column_data_types, connection)
    # Import data into MySQL table
    import_data(csv_filename, table_name, connection)

# Close MySQL connection
connection.close()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.sql.types import IntegerType, StringType
import pandas as pd
import pypyodbc as odbc


# ---------- CONFIG ---------- 
CSV_FILE_PATH = "data\\ad_click_dataset.csv" 
SQLSERVER_CONFIG = {
    "DRIVER": "<drivername>",
    "SERVER_NAME": "<servername>",  
    "DATABASE_NAME": "<Database name>", 
    "TABLE_NAME": "<table name>" 
}

# ---------- SPARK SESSION ----------
spark = SparkSession.builder.appName("CSV_to_SQLServer_ETL").getOrCreate()

# ---------- EXTRACT STEP ----------
def extract_data_from_csv(csv_file_path):
    print(f"Extracting data from: {csv_file_path}")
    return spark.read.csv(csv_file_path, header=True, inferSchema=True)

# ---------- TRANSFORM STEP ----------
def transform_data(df):
    
    print("Transforming data...")

    # Drop rows that are completely NA
    df_cleaned = df.dropna(how='all')

    # Fill missing values for nullable columns
    df_cleaned = df_cleaned.fillna({
        'user_id': 'NULL',
        'age': 0,
        'gender': 'NULL',
        'device_type': 'NULL',
        'ad_position': 'NULL',
        'browsing_history': 'NULL',
        'time_of_day': 'NULL',
        'click': 0
    })

    # Explicitly cast each column to the correct data type
    df_cleaned = df_cleaned.select(
        col('id').cast(IntegerType()),
        col('user_id').cast(StringType()),
        col('age').cast(IntegerType()),
        col('gender').cast(StringType()),
        col('device_type').cast(StringType()),
        col('ad_position').cast(StringType()),
        col('browsing_history').cast(StringType()),
        col('time_of_day').cast(StringType()),
        col('click').cast(IntegerType())
    )

    # Reorder the columns to match the SQL insert order
    column_order = ['id', 'user_id', 'age', 'gender', 'device_type',
                    'ad_position', 'browsing_history', 'time_of_day', 'click']
    df_cleaned = df_cleaned[column_order]

    print("Transformation is complete!")
    return df_cleaned


# ---------- LOAD TO SQL SERVER ----------
def load_data_to_sql_server(df, config):
    print("Loading data to SQL Server...")

    # Convert Spark DataFrame to Pandas DataFrame for insertion into SQL Server
    df_pd = df.toPandas()

    # Establish SQL Server connection using PyODBC
    conn_string = f"""
        DRIVER={{{config['DRIVER']}}};
        SERVER={config['SERVER_NAME']};
        DATABASE={config['DATABASE_NAME']};
        Trust_Connection=yes;
    """
    try:
        conn = odbc.connect(conn_string)
        cursor = conn.cursor()
        print(type(df_pd.columns))
        # 1. Create table if it doesn't exist
        create_table_sql = f"""
            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name = '{config['TABLE_NAME']}' AND xtype = 'U')
            CREATE TABLE {config['TABLE_NAME']} (
                id INT,
                user_id NVARCHAR(50),
                age INT,
                gender NVARCHAR(50),
                device_type NVARCHAR(50),
                ad_position NVARCHAR(50),
                browsing_history NVARCHAR(255),
                time_of_day NVARCHAR(50),
                click INT
            )
        """
        cursor.execute(create_table_sql)
        conn.commit()

        print("Table created or already exists.")

        # 2. Prepare the SQL INSERT statement with explicit column names
        insert_sql = f"""
            INSERT INTO {config['TABLE_NAME']} (id, user_id, age, gender, device_type, ad_position, browsing_history, time_of_day, click)
            VALUES ({', '.join(['?'] * len(df_pd.columns))})
        """
        print("I'm here!")
        # 3. Insert the data into the table
        for row in df_pd.values.tolist()[:5]:  # prints first 5 rows
            print(row)

        cursor.executemany(insert_sql, df_pd.values.tolist())
        cursor.commit()
        print("Data inserted successfully.")
    
    except Exception as e:
        print("Error:", str(e))
        cursor.rollback()
    
    finally:
        cursor.close()
        conn.close()

# ---------- MAIN ----------
def main():
    # Step 1: Extract data from CSV
    df_spark = extract_data_from_csv(CSV_FILE_PATH)

    # Step 2: Transform (clean) data
    df_transformed = transform_data(df_spark)

    # Step 3: Load transformed data into SQL Server
    load_data_to_sql_server(df_transformed, SQLSERVER_CONFIG)
    
    print("ETL job complete.")

if __name__ == "__main__":
    main()

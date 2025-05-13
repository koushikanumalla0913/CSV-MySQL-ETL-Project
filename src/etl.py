from spark_processor import initialize_spark, load_csv_to_spark, clean_data
from mysql_loader import load_to_mysql
from dw_loader import load_to_dw
import logging

def run_etl_pipeline(csv_path):
    # Set up logging
    logging.basicConfig(filename='logs/etl_process.log', level=logging.INFO)
    
    # Step 1: Initialize Spark session
    spark = initialize_spark()
    
    # Step 2: Load data from CSV using PySpark
    df = load_csv_to_spark(spark, csv_path)
    
    # Step 3: Clean the data
    df_cleaned = clean_data(df)
    
    # Step 4: Load data into MySQL
    load_to_mysql(df_cleaned)
    logging.info("Data loaded to MySQL successfully.")
    
    # Step 5: Load data into Data Warehouse (Optional)
    #load_to_dw(df_cleaned)
    #logging.info("Data loaded to Data Warehouse successfully.")


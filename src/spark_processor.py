from pyspark.sql import SparkSession

def initialize_spark():
    spark = SparkSession.builder.appName("CSV_to_MySQL_ETL").getOrCreate()
    return spark

def load_csv_to_spark(spark, file_path):
    # Load the CSV file into PySpark DataFrame
    df = spark.read.csv(file_path, header=True, inferSchema=True)
    return df

def clean_data(df):
    # Perform data cleaning tasks (e.g., dropna, fillna)
    df_cleaned = df.dropna(subset=["id", "full_name"])  # Example of removing rows with missing id/full_name
    df_cleaned = df_cleaned.fillna({"gender": "Unknown", "age": 0})
    return df_cleaned

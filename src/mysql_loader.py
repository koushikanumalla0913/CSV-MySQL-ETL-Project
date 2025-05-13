import pandas as pd
from sqlalchemy import create_engine
from config import MYSQL_USER, MYSQL_PASSWORD, MYSQL_HOST, MYSQL_DATABASE

def load_to_mysql(df):
    # Convert PySpark DataFrame to Pandas DataFrame for MySQL compatibility
    pandas_df = df.toPandas()
    
    # Create SQLAlchemy engine for MySQL connection
    engine = create_engine(f'mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}/{MYSQL_DATABASE}')
    
    # Load data into MySQL
    pandas_df.to_sql('user_data', con=engine, if_exists='replace', index=False)

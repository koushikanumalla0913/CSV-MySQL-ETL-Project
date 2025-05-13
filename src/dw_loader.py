# import psycopg2
# from config import REDSHIFT_HOST, REDSHIFT_DB, REDSHIFT_USER, REDSHIFT_PASSWORD

# def load_to_dw(df):
#     # Connect to Redshift (or other Data Warehouse)
#     conn = psycopg2.connect(
#         host=REDSHIFT_HOST,
#         dbname=REDSHIFT_DB,
#         user=REDSHIFT_USER,
#         password=REDSHIFT_PASSWORD
#     )
#     cursor = conn.cursor()

#     # Example Redshift SQL to insert data
#     for index, row in df.iterrows():
#         cursor.execute("""
#         INSERT INTO user_data (id, full_name, age, gender, device_type, ad_position, browsing_history, time_of_day, click)
#         VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
#         """, (row['id'], row['full_name'], row['age'], row['gender'], row['device_type'], row['ad_position'], row['browsing_history'], row['time_of_day'], row['click']))

#     conn.commit()
#     cursor.close()
#     conn.close()

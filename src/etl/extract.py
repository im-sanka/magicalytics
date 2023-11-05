# + tags=["parameters"]
# declare a list tasks whose products you want to use as inputs
# testing workflow 

upstream = None

# -

from google.cloud import bigquery
import os
import duckdb
# import md_token
import logging

# +
logging.basicConfig(filename='script_report.txt', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# +
def get_data_bigquery(country:str, credentials):
    """
    Pull data from BigQuery and return it as a dataframe.
    """
    client = bigquery.Client(credentials=credentials)

    query = f"""
                SELECT refresh_date, region_name, score, term, rank, COUNT(rank) as group_in_rank, percent_gain
                FROM `bigquery-public-data.google_trends.international_top_rising_terms`
                WHERE country_name = '{country}'
                GROUP BY refresh_date, region_name, score, rank, term, percent_gain
            """
    
    query_job = client.query(query)
    df = query_job.to_dataframe()
    df['refresh_date'] = df['refresh_date'].astype(str)

    return df

# +
def add_or_update_duckdb(name, df, table_name):
    """
    Either add new database or update the database from BigQuery data
    """
    folder_path = 'output'
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)

    db_path = os.path.join(folder_path, f'{name}.duckdb')
    conn = duckdb.connect(db_path)
    conn.register('df', df)
    
    # Check if table exists and decide whether to create a new table or insert data into the existing table
    result = conn.execute(f"SELECT * FROM information_schema.tables WHERE table_name = '{table_name}';").fetchall()
    if result:
        # If the table exists, just insert new data without deleting the existing data
        conn.execute(f"INSERT INTO \"{table_name}\" SELECT * FROM df;")
    else:
        # If the table doesn't exist, create it and insert the data
        conn.execute(f'CREATE TABLE "{table_name}" AS SELECT * FROM df;')

    return conn

# -

if __name__ == "__main__":
    logging.info("ETL process started.")
    
    """
    Main ETL commands for data ingestion and pushing the table into the clean database.
    """
    try:
        if "GOOGLE_APPLICATION_CREDENTIALS" not in os.environ:
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/immanuelsanka/Desktop/Medium/magicalytics/.bigquery/magicalytics.json"
            logging.info("Credentials set successfully.")
    except Exception as e:
        logging.error(f"Error setting credentials: {e}")

    try:
        df = get_data_bigquery("Indonesia", os.environ["GOOGLE_APPLICATION_CREDENTIALS"])
        logging.info("Data successfully fetched from BigQuery.")
    except Exception as e:
        logging.error(f"Error fetching data from BigQuery: {e}")

    try:
        conn = add_or_update_duckdb("clean_data", df, "all")

        # Ensure to handle the 'java_data' table as well, checking its existence and updating/creating as needed.
        java_data_exists = conn.execute("SELECT * FROM information_schema.tables WHERE table_name = 'java_data';").fetchall()
        if java_data_exists:
            query = """
                    SELECT * 
                    FROM "df" 
                    WHERE region_name IN ('Central Java', 'Special Region of Yogyakarta', 
                    'Special Capital Region of Jakarta', 'Banten', 'Bali', 'West Java', 'East Java')
                    """
            conn.execute(f'CREATE OR REPLACE TABLE "java_data" AS {query}')
            df2 = conn.execute("SELECT * FROM java_data").fetch_df()
        logging.info("Data added or updated in DuckDB.")
        
    except Exception as e:
        logging.error(f"Error adding/updating data in DuckDB: {e}")
     
    """
    Push the processed data to motherduck!
    """

    if 'MD_TOKEN' in os.environ:
        token = os.environ['MD_TOKEN']
    elif 'MD_TOKEN' in environ:  # Assuming you've done "from os import environ" somewhere
        token = environ['MD_TOKEN']
    else:
        token = "Token is not available!"
        logging.warning("MD_TOKEN not available.")
    
    cloud = duckdb.connect(f"md:clouddb?motherduck_token={token}") 
    cloud.execute("LOAD motherduck")
    cloud.execute("CREATE OR REPLACE TABLE 'clouddb.main.df1' AS SELECT * FROM 'df'")
    cloud.execute("CREATE OR REPLACE TABLE 'clouddb.main.df2' AS SELECT * FROM 'df2'")
    

    conn.sql("SHOW DATABASES").show()
    cloud.sql("SHOW DATABASES").show()

    """
    Close all connections!
    """

    conn.close()
    cloud.close()
    
    try:
        print("Extract, Transform and Load are done!")
        logging.info("ETL process completed successfully.")
    except Exception as e:
        logging.error(f"Error in ETL process: {e}")

# # + tags=["parameters"]
# # declare a list tasks whose products you want to use as inputs
# upstream = None


# # +
# import requests 
# import pandas as pd
# import json
# import duckdb

# # +
# def extract_data(url):
#     """Extract data from URL and return a dataframe"""
#     response = requests.get(url)
#     if response.status_code == 200:
#         return pd.DataFrame(json.loads(response.content))
#     else:
#         raise Exception(f"Error retrieving data from {url}")

# # +
# # write a function that saves a dataframe to duckdb
# def save_to_duckdb(df, table_name, db_path):
#     """Save dataframe to duckdb"""
#     conn = duckdb.connect(db_path)
#     conn.register('df', df)
#     conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")
#     conn.close()

# # +
# if __name__ == "__main__":

#     # Extract data from URL
#     url = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"
#     df = extract_data(url)
    
#     # Save to duckdb
#     db_path = "data.duckdb"
#     table_name = "nycitydata"
#     save_to_duckdb(df, table_name, db_path)


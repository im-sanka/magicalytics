# after browsing the database, turned out the data source is not from Kaggle but bigquery public data
# import kaggle
# kaggle.api.authenticate()
# kaggle.api.dataset_download_files('data:google_analytics_sample', path='./', unzip=True)

from google.cloud import bigquery
import os
import duckdb
from credentials import credentials

def get_data_bigquery(query, credentials):
    """
    This works to pull the data from BigQuery and make it as a dataframe.
    """
    credentials
    client = bigquery.Client()
    query_job = client.query(query)
    df = query_job.to_dataframe()
    df = df.sort_values(by='rank')
    df['refresh_date'] = df['refresh_date'].astype(str)
    return df

def add_data_to_duckdb(df, table_schema):
    """
    This puts the dataframe into duckdb format where it will host many tables based on ranks.
    So, it has 25 tables in the final database.
    """
    folder_path = 'data_folder'  # Specify your desired folder name here
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)  # Create the folder if it doesn't exist

    db_path = os.path.join(folder_path, 'test_data.duckdb')
    con = duckdb.connect(database=db_path, read_only=False)

    return con

if __name__ == "__main__":
    """
    This is the main ETL commands  where it will execute the data ingestion and push the table into the database
    """
    df = get_data_bigquery("""
        SELECT refresh_date, region_name, score, term, rank, COUNT(rank) as group_in_rank, percent_gain
        FROM `bigquery-public-data.google_trends.international_top_rising_terms`
        WHERE country_name = 'Indonesia' 
        GROUP BY refresh_date, region_name, score, rank, term, percent_gain
        """, credentials())

    table_schema = """
    (
        refresh_date VARCHAR,
        region_name VARCHAR,
        score FLOAT,
        term VARCHAR,
        rank INT,
        group_in_rank INT,
        percent_gain FLOAT
    )
    """

    con = add_data_to_duckdb(df, table_schema)
    print("Query done!")

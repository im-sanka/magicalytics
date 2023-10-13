# + tags=["parameters"]
# declare a list tasks whose products you want to use as inputs
upstream = None

# -

from google.cloud import bigquery
import os
import duckdb
# from credentials import credentials

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
def add_to_duckdb(name, df, table_name):
    folder_path = 'output'
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)

    db_path = os.path.join(folder_path, f'{name}.duckdb')
    conn = duckdb.connect(db_path)
    conn.register('df', df)
    conn.execute(f'CREATE TABLE "{table_name}" AS SELECT * FROM df')

    return conn


# -

if __name__ == "__main__":
    """
    Main ETL commands for data ingestion and pushing the table into the clean database.
    """
    def credentials():
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/immanuelsanka/Desktop/Medium/magicalytics/.bigquery/magicalytics.json"
    df = get_data_bigquery("Indonesia", credentials())
    conn = add_to_duckdb("clean_data", df, "all")

    query = """
            CREATE TABLE java_data AS    
            SELECT * 
            FROM "all" 
            WHERE region_name IN ('Central Java', 'Special Region of Yogyakarta', 
            'Special Capital Region of Jakarta', 'Banten', 'Bali', 'West Java', 'East Java')
            """
    
    conn.execute(query)
    conn.close()

    print("Extract and Transform are done!")

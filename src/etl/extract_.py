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
        conn.execute(f"DELETE FROM \"{table_name}\";")  # Clear existing table contents
        conn.execute(f"INSERT INTO \"{table_name}\" SELECT * FROM df;")  # Insert new data
    else:
        conn.execute(f'CREATE TABLE "{table_name}" AS SELECT * FROM df')  # Create table if it does not exist

    return conn

# -

if __name__ == "__main__":
    """
    Main ETL commands for data ingestion and pushing the table into the clean database.
    """
    def credentials():
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/immanuelsanka/Desktop/Medium/magicalytics/.bigquery/magicalytics.json"
    df = get_data_bigquery("Indonesia", credentials())

    conn = add_or_update_duckdb("clean_data", df, "all")

    # Ensure to handle the 'java_data' table as well, checking its existence and updating/creating as needed.
    java_data_exists = conn.execute("SELECT * FROM information_schema.tables WHERE table_name = 'java_data';").fetchall()
    if java_data_exists:
        conn.execute("DELETE FROM java_data;")
    
    query = """
            INSERT INTO java_data    
            SELECT * 
            FROM "all" 
            WHERE region_name IN ('Central Java', 'Special Region of Yogyakarta', 
            'Special Capital Region of Jakarta', 'Banten', 'Bali', 'West Java', 'East Java')
            """
    
    conn.execute(query)
    conn.close()

    """
    Need to add load function to push downloaded data to motherduck here
    """

    print("Extract and Transform are done!")

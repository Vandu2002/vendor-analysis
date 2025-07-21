import pandas as pd
import os
from sqlalchemy import create_engine
import logging
import time

# Logging setup
logging.basicConfig(
    filename="logs/ingestion_db.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

# Create SQLite engine
engine = create_engine('sqlite:///inventory.db')

def ingest_db(df, table_name, engine):
    '''This function will ingest the dataframe into a database table.'''
    df.to_sql(table_name, con=engine, if_exists='replace', index=False)

def load_raw_data():
    '''This function loads CSV files as DataFrames and ingests them into the DB.'''
    start = time.time()

    for file in os.listdir('data'):
        if file.endswith('.csv'):
            try:
                df = pd.read_csv(os.path.join('data', file))
                logging.info(f'Ingesting {file} into the database')
                ingest_db(df, file[:-4], engine)
            except Exception as e:
                logging.error(f"Failed to ingest {file}: {e}")

    # These should be outside the loop
    end = time.time()
    total_time = (end - start) / 60
    logging.info('--------------Ingestion Complete------------') 
    logging.info(f'Total Time taken: {total_time:.2f} minutes')

if __name__ == '__main__':
    load_raw_data()

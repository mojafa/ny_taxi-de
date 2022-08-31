import os

from time import time

import pandas as pd
import pyarrow.parquet as pq

from sqlalchemy import create_engine


def ingest_callable(user, password, host, port, db, table_name, filename, execution_date):
    print(table_name, filename,execution_date)

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()
    engine.execute(f'DROP TABLE IF EXISTS {table_name}')
    print('connection established successfully, inserting data...')

    t_start = time()
    df = pd.read_parquet(filename)
    parquet_table = pq.read_table(filename)
    df = parquet_table.to_pandas()
    df.to_csv(table_name,index=False, sep='\t')
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    df_iter = pd.read_csv(table_name, iterator=True, chunksize=100000)
   
    df = next(df_iter)

    
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
    # df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    # df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    df.to_sql(name=table_name, con=engine, if_exists='replace')

    t_end = time()
    print('inserted the first chunk, took %.3f second' % (t_end - t_start))

    while True: 
        t_start = time()

        try:
            df = next(df_iter)
        except StopIteration:
            print("completed")
            break

        # df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        # df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

        df.to_sql(name=table_name, con=engine, if_exists='replace')

        t_end = time()

        print('inserted another chunk, took %.3f second' % (t_end - t_start))

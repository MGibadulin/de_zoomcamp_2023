#!/usr/bin/env python
# coding: utf-8

import os
import argparse

from time import time

import pandas as pd
from sqlalchemy import create_engine


def main(params):
    user = params.user
    password = params.password
    host = params.host 
    port = params.port 
    db = params.db
    
    table_name_trip = "trip"
    table_name_zone = "zone"
    
    url_trip_data = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz"
    url_zone_data = "https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv"
    
    csv_name_trip = 'trip.csv.gz'
    csv_name_zone = 'zone.csv'

    # save data on local storage
    os.system(f"wget {url_trip_data } -O {csv_name_trip}")
    os.system(f"wget {url_zone_data} -O {csv_name_zone}")

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # trips
    print("Start ingesting trip data into the postgres database")
    df_iter = pd.read_csv(csv_name_trip, iterator=True, chunksize=100000)

    df = next(df_iter)
    
    # fix type of columns
    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
    
    print(f"create table '{table_name_trip}'")
    df.head(n=0).to_sql(name=table_name_trip, con=engine, if_exists='replace')
    
    t_start = time()
    df.to_sql(name=table_name_trip, con=engine, if_exists='append')
    t_end = time()

    print('inserted first chunk, took %.3f second' % (t_end - t_start))

    while True: 

        try:
            t_start = time()
            
            df = next(df_iter)

            df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
            df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

            df.to_sql(name=table_name_trip, con=engine, if_exists='append')

            t_end = time()

            print('inserted another chunk, took %.3f second' % (t_end - t_start))

        except StopIteration:
            print("Finished ingesting trip data into the postgres database")
            break
    
    # zones
    print("Start ingesting zone data into the postgres database")
    df = pd.read_csv(csv_name_zone)
    df.to_sql(name=table_name_zone, con=engine, if_exists='replace')
    print("Finished ingesting zone data into the postgres database")
    
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')

    args = parser.parse_args()

    main(args)

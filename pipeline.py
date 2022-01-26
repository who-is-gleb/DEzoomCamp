#!/usr/bin/env python
# coding: utf-8

import os

import pandas as pd
from sqlalchemy import create_engine
import argparse


def main(params):
    username = params.username
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table = params.table
    url = params.url

    # Step 1: download csv
    # csv_name = 'downloaded_data.csv'
    csv_name = 'downloaded_data_zones.csv'

    os.system(f'wget {url} -O {csv_name}')

    # Step 2:
    engine = create_engine(f'postgresql://{username}:{password}@{host}:{port}/{db}')
    engine.connect()

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)
    df = next(df_iter)
    print(df.head)
    # df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    # df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    df.head(n=0).to_sql(name=table, con=engine, if_exists='replace')
    df.to_sql(name=table, con=engine, if_exists='append')

    while True:
        df = next(df_iter)
        # df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        # df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
        df.to_sql(name=table, con=engine, if_exists='append')
        print("inserted another chunk...")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest data')
    parser.add_argument('--username', help='postgres username')
    parser.add_argument('--password', help='postgres password')
    parser.add_argument('--host', help='postgres host')
    parser.add_argument('--port', help='postgres port')
    parser.add_argument('--db', help='postgres db name')
    parser.add_argument('--table', help='postgres table name')
    parser.add_argument('--url', help='csv file url')

    args = parser.parse_args()

    main(args)
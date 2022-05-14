#!/usr/bin/env python
# My attempt to save the data in an sqlite file instead
# of the sql dbase
# coding: utf-8
import pandas as pd
import sqlite3
import os
from IPython import get_ipython

yellow_data=os.path.join("/media/cap/7fed51bd-a88e-4971-9656-d617655b6312/data/sources/data-engineering-zoomcamp",'yellow_tripdata_2021-01.csv')
df = pd.read_csv(yellow_data, nrows=100)

df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

df_iter = pd.read_csv(yellow_data,iterator=True, chunksize=100000)
df = next(df_iter)
df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
dbase="test.sqlite"
engine = sqlite3.connect(dbase)
df.head(n=0).to_sql(name='yellow_taxi_data', con=engine, if_exists='replace')

#get_ipython().run_line_magic('time', "df.to_sql(name='yellow_taxi_data', con=engine, if_exists='append')")


from time import time

while True: 
    t_start = time()

    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    
    df.to_sql(name='yellow_taxi_data', con=engine, if_exists='append')

    t_end = time()

    print('inserted another chunk, took %.3f second' % (t_end - t_start))

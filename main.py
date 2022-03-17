
from functions import *
import requests
import time
import itertools
import os
import datetime as dt
    
import config
# import csv
import psycopg2
import psycopg2.extras


connection = psycopg2.connect(host=config.DB_HOST, database=config.DB_NAME, user=config.DB_USER, password=config.DB_PASS)
cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)                      


# dates = os.listdir('data')
# dates = [date for date in dates if date[0]!='.']
# for current_date in dates:
#     insert_holdings_into_database(current_date, connection, cursor)


current_date = download_csvs()
insert_holdings_into_database(current_date, connection, cursor)


# list all companies sorted by market values
market_values = list_companies_by_value(cursor)


# # compare only 2 recent dates - get the dates from the database
# cursor.execute("select distinct(dt) from etf_holding ORDER BY dt DESC LIMIT 2")
# dates = cursor.fetchall()
# date_current= dates[0][0].strftime("%Y-%m-%d")
# date_previous = dates[1][0].strftime("%Y-%m-%d")


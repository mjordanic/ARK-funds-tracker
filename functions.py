import requests
import time
import itertools
import os
import datetime as dt
import math

import pandas as pd
import numpy as np

import config
import csv
import psycopg2
import psycopg2.extras


def download_csvs():
    """
    The function downloads the CSV files with fund holdings from https://ark-funds.com.
    The files are stored to the folder data/current_date.
    Each CSV file corresponds to one ETF, where the name of the csv file is also the name of the fund. 
    Args:
        cursor  - cursor connected to the database using psycopg2 package
                - (cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor))
    Returns:
        date    - current date on which the files were downloaded
    """

    headers = [{'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}]

    # In case that user agents have to be changed. It works without it.
    # headers = [{'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'},
    #            {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36'},
    #            {'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2919.83 Safari/537.36'},
    #           ]
    headers_cycle = itertools.cycle(headers)

    etf_urls = {'ARKK' : 'https://ark-funds.com/wp-content/uploads/funds-etf-csv/ARK_INNOVATION_ETF_ARKK_HOLDINGS.csv',
                'ARKQ' : 'https://ark-funds.com/wp-content/uploads/funds-etf-csv/ARK_AUTONOMOUS_TECH._&_ROBOTICS_ETF_ARKQ_HOLDINGS.csv',
                'ARKW' : 'https://ark-funds.com/wp-content/uploads/funds-etf-csv/ARK_NEXT_GENERATION_INTERNET_ETF_ARKW_HOLDINGS.csv',
                'ARKG' : 'https://ark-funds.com/wp-content/uploads/funds-etf-csv/ARK_GENOMIC_REVOLUTION_ETF_ARKG_HOLDINGS.csv',
                'ARKF' : 'https://ark-funds.com/wp-content/uploads/funds-etf-csv/ARK_FINTECH_INNOVATION_ETF_ARKF_HOLDINGS.csv',
                'ARKX' : 'https://ark-funds.com/wp-content/uploads/funds-etf-csv/ARK_SPACE_EXPLORATION_&_INNOVATION_ETF_ARKX_HOLDINGS.csv',
                'PRNT' : 'https://ark-funds.com/wp-content/uploads/funds-etf-csv/THE_3D_PRINTING_ETF_PRNT_HOLDINGS.csv',
                }

    #connection = psycopg2.connect(host=config.DB_HOST, database=config.DB_NAME, user=config.DB_USER, password=config.DB_PASS)
    #cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)

    etfs = etf_urls.keys()

    date = dt.date.today().strftime("%Y-%m-%d")
    date_path = f"data/{date}/"

    if not os.path.exists(date_path):
            os.mkdir(date_path)

    for etf, header in zip(etfs, headers_cycle):
        #time.sleep(0.3) # wait 300 ms
        
        try: 
            response = requests.get(etf_urls[etf], headers=header)
        except Exception as e:
            print('DOWNLOAD ERROR:')
            print(e)
            print(etf)
        
        # save to csv file
        with open(f"{date_path}{etf}.csv", 'w') as f:
            f.write(response.text)

    return date


def insert_holdings_into_database(current_date, connection, cursor):
    """
    The function inserts the data from the downloaded CSV files into the database.
    
    Args:
        current_date - string containing the current date (returned by the function 'download_csvs')
        connection  - connection to the database using psycopg2 package
                    - connection = psycopg2.connect(host=config.DB_HOST, database=config.DB_NAME, user=config.DB_USER, password=config.DB_PASS)
        cursor  - cursor connected to the database using psycopg2 package
                - (cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor))
    Returns:
        None
    """

    list_dirs = os.listdir(f'data/{current_date}')
    etfs = [dir[:-4] for dir in list_dirs if dir[-3:]=='csv']
    print(etfs)
    for etf in etfs:
        print(etf)

        with open(f"data/{current_date}/{etf}.csv") as f:
            reader = csv.reader(f)
            next(reader)
            for row in reader:
                
                # the last row of tables have only one column
                if len(row)==1:
                    break

                stock_name = row[2]
                
                # valid row in csv file
                if stock_name: 

                    stock_ticker = row[3]
                    stock_cusip = row[4]
                    
                    stock_shares = row[5]
                    stock_shares = int(stock_shares.replace(',', ''))

                    stock_market_value = row[6]
                    if stock_market_value[0]=='$':
                        stock_market_value = stock_market_value[1:]
                        stock_market_value = float(stock_market_value.replace(',', ''))
                    
                    stock_weight = row[7]
                    if stock_weight[-1] == '%':
                        stock_weight = stock_weight[:-1]
                        stock_weight = float(stock_weight.replace(',', '')) / 100
                    
                    # Check if it exists in stocks table
                    cursor.execute("""
                        SELECT * FROM stocks WHERE ticker = %s
                    """, (stock_ticker,))
                    stock = cursor.fetchone()

                    # if this ticker exists in stock database
                    if stock:
                        cursor.execute("""
                            INSERT INTO etf_holdings (dt, fund, stock_id, shares, market_value, weight)
                            VALUES (%s, %s, %s, %s, %s, %s)
                            """, (current_date, etf, stock['id'], stock_shares, stock_market_value, stock_weight))
                        connection.commit()
     
                    # if ticker exists, but is not in database
                    elif stock_ticker:
                        cursor.execute("""
                                INSERT INTO stocks (company, ticker, cusip) 
                                VALUES (%s, %s, %s)
                            """, (stock_name, stock_ticker, stock_cusip))
                        connection.commit()
                        cursor.execute("""
                                SELECT * FROM stocks WHERE ticker = %s
                                """, (stock_ticker,))
                        stock = cursor.fetchone()
                        cursor.execute("""
                            INSERT INTO etf_holdings (dt, fund, stock_id, shares, market_value, weight)
                            VALUES (%s, %s, %s, %s, %s, %s)
                            """, (current_date, etf, stock['id'], stock_shares, stock_market_value, stock_weight))
                        connection.commit()

                    # if it doesn't have a ticker
                    else:
                        # check if the stock with same name already exists in database
                        cursor.execute("""
                                SELECT * FROM stocks WHERE company = %s
                                """, (stock_name,))
                        stock = cursor.fetchone()
                        if stock: # and now insert into the db
                            cursor.execute("""
                                INSERT INTO etf_holdings (dt, fund, stock_id, shares, market_value, weight)
                                VALUES (%s, %s, %s, %s, %s, %s)
                                """, (current_date, etf, stock['id'], stock_shares, stock_market_value, stock_weight))
                            connection.commit()

                        else:
                            # if it doesn't exist, create a new ticker
                            stock_ticker = '$'+stock_name[:4]
                            
                            # Make sure that the ticker doesn't exist already
                            ticker_exists = 1
                            loop_no = 0
                            while ticker_exists:
                                cursor.execute("""
                                        SELECT * FROM stocks WHERE ticker = %s
                                        """, (stock_ticker,))
                                stock = cursor.fetchone()
                                if not(stock):
                                    break
                                loop_no = loop_no + 1
                                stock_ticker = stock_ticker+str(loop_no)

                            cursor.execute("""
                                    INSERT INTO stocks (company, ticker, cusip) 
                                    VALUES (%s, %s, %s)
                                """, (stock_name, stock_ticker, stock_cusip))
                            connection.commit()

                            cursor.execute("""
                                    SELECT * FROM stocks WHERE ticker = %s
                                    """, (stock_ticker,))
                            stock = cursor.fetchone()
                            cursor.execute("""
                                INSERT INTO etf_holdings (dt, fund, stock_id, shares, market_value, weight)
                                VALUES (%s, %s, %s, %s, %s, %s)
                                """, (current_date, etf, stock['id'], stock_shares, stock_market_value, stock_weight))
                            connection.commit()


def list_companies_by_value(cursor):

    cursor.execute("select distinct(dt) from etf_holdings ORDER BY dt DESC LIMIT 1")
    dates = cursor.fetchall()
    date_current= dates[0][0].strftime("%Y-%m-%d")

    cursor.execute("""
        SELECT t2.ticker, t2.company, SUM(t1.market_value)
        FROM etf_holdings t1 JOIN stocks t2 ON t1.stock_id = t2.id
        WHERE t1.dt=%s
        GROUP BY t2.ticker, t2.company
        ORDER BY SUM(t1.market_value) DESC
        """, (date_current,))
    market_values = cursor.fetchall()
    
    initial_message = f'-\n -\n -\n -\n ----------------{date_current}---------------------------\nTICKER  --  COMPANY  --  VALUE PCT  --  (FUND PERC (PERC IN FUND))'
    send_discord_message(initial_message)
    
    message = []
    total_value = sum([int(value[2]) for value in market_values])

    for row in market_values:
        ticker = row[0]
        company = row[1]
        market_value = int(row[2])
        value_pct = market_value/total_value*100
        message_row = [ticker, company, f"{value_pct:.2f}%"]

        cursor.execute("""
            SELECT fund, shares, weight 
            FROM etf_holdings
            WHERE dt=%s and stock_id=(SELECT id FROM stocks WHERE ticker=%s)
            ORDER BY shares DESC
        """, (date_current,ticker))
        individual_share = cursor.fetchall()
        total_shares = sum([ind_share[1] for ind_share in individual_share])
        message_ind = []
        for ind_share in individual_share:
            message_ind.append(f"{ind_share[0]} {ind_share[1]/(total_shares+1e-15)*100:.2f}% ({100*float(ind_share[2])}%) ")

        message_ind = '('+ (', ').join(message_ind) +')'
        message_row.append(message_ind)
        message_row = (' -- ').join(message_row)
        message.append(message_row) 

    number_of_rows = len(message)
    for i in range(math.ceil(number_of_rows/15.0)):
        
        if number_of_rows-i*15>15:
            message_send = message[i*15:(i+1)*15]
        else:
            message_send = message[i*15:]
        message_send = ('\n').join(message_send) 
        
        send_discord_message(message_send)

    return market_values



def list_differences(cursor):

    cursor.execute("select distinct(dt) from etf_holdings ORDER BY dt DESC LIMIT 2")
    dates = cursor.fetchall()
    date_current= dates[0][0].strftime("%Y-%m-%d")
    date_previous = dates[1][0].strftime("%Y-%m-%d")


    cursor.execute("""
        SELECT * FROM etf_holdings WHERE dt= %s
        """, (date_current,))
    current_holding = cursor.fetchall()

    cursor.execute("""
        SELECT * FROM etf_holdings WHERE dt= %s
        """, (date_previous,))
    prev_holding = cursor.fetchall()

    cursor.execute("""
        SELECT id, ticker, company FROM stocks
        """)
    stocks_data = cursor.fetchall()
    stocks_data = pd.DataFrame(stocks_data, columns=['id', 'ticker', 'company']).set_index('id')

    prev_holding = [[i[0], i[1], i[2], i[3], float(i[4]), float(i[5])]for i in prev_holding]
    current_holding = [[i[0], i[1], i[2], i[3], float(i[4]), float(i[5])]for i in current_holding]
    current_holding = pd.DataFrame(current_holding, columns=['date', 'fund', 'index', 'shares', 'value', 'percent'])
    prev_holding = pd.DataFrame(prev_holding, columns=['date', 'fund', 'index', 'shares', 'value', 'percent'])

    merged = current_holding.merge(prev_holding, how='outer', left_on=['fund', 'index'], right_on=['fund', 'index'])
    merged['diff_shares'] = merged['shares_x']/merged['shares_y'] - 1 
    merged['diff_value'] = merged['value_x']/merged['value_y'] - 1 
    merged['diff_percent'] = merged['percent_x']/merged['percent_y'] - 1 

    grouped = merged.groupby('index')[['shares_x', 'shares_y', 'value_x', 'value_y', 'percent_x', 'percent_y']].sum()

    grouped['diff_shares'] = grouped['shares_x']/grouped['shares_y'] - 1 
    grouped['diff_value'] = grouped['value_x']/grouped['value_y'] - 1 
    grouped['diff_percent'] = grouped['percent_x']/grouped['percent_y'] - 1 

    grouped['abs_diff_shares'] = grouped['diff_shares'].abs()
    grouped.sort_values('abs_diff_shares', ascending=False, inplace=True)
    grouped.dropna(subset='abs_diff_shares', inplace=True)

    grouped['total_value_percent'] = grouped['value_x']/grouped['value_x'].sum()

    message = []
    for row in grouped.iterrows():
        stock_id = row[0]
        ticker = stocks_data['ticker'][stock_id]
        company = stocks_data['company'][stock_id]
        stock_diff_shares = row[1]['abs_diff_shares']
        if stock_diff_shares==-1:
            stock_diff_shares="Removed"
        elif stock_diff_shares==np.inf:
            stock_diff_shares="Added"
        else:
            if stock_diff_shares>0:
                stock_diff_shares=f"^{100*stock_diff_shares:.2f}%"
            else:
                stock_diff_shares=f"{100*stock_diff_shares:.2f}%"
        
        stock_diff_value_pct = f"{100*row[1]['diff_value']:.2f}%"
        stock_value_pct = f"{100*row[1]['total_value_percent']:.2f}"
        
        message_row = [ticker, company, stock_diff_shares, stock_diff_value_pct, stock_value_pct]
        
        merged_stock = merged.loc[merged['index']==stock_id].copy()
        merged_stock['abs_diff_shares'] = merged_stock['diff_shares'].abs()
        merged_stock.sort_values('abs_diff_shares', ascending=False, inplace=True)
        merged_stock.dropna(subset='abs_diff_shares', inplace=True)
        merged_stock[['fund', 'abs_diff_shares', 'value_x', 'percent_x']].values
        
        message_ind = []
        for fund_rows in merged_stock.iterrows():
            fund_name = fund_rows[1]['fund']
            
            fund_diff_shares = fund_rows[1]['abs_diff_shares']
            if fund_diff_shares==-1:
                fund_diff_shares="Removed"
            elif fund_diff_shares==np.inf:
                fund_diff_shares="Added"
            else:
                if fund_diff_shares>0:
                    fund_diff_shares=f"^{100*fund_diff_shares:.2f}%"
                else:
                    fund_diff_shares=f"{100*fund_diff_shares:.2f}%"
                
            fund_value = fund_rows[1]['value_x']/1e6
            
            message_ind.append(f"{fund_name} {fund_diff_shares} (M${fund_value:.1f})")

        message_ind = '('+ (', ').join(message_ind) +')'
        message_row.append(message_ind)
        message_row = (' -- ').join(message_row)
        message.append(message_row) 

    initial_message = f'-\n -\n -\n -\n ----------------{date_current}---------------------------\nTICKER  --  COMPANY  --  CHANGE in SHARES  --  CHANGE in VALUE  --  CURRENT PCT of TOTAL VALUE  --  (FUND, SHARE CHANGE, (VALUE))'
    send_discord_message(initial_message, ch='differences')
    

    number_of_rows = len(message)
    for i in range(math.ceil(number_of_rows/15.0)):
        
        if number_of_rows-i*15>15:
            message_send = message[i*15:(i+1)*15]
        else:
            message_send = message[i*15:]
        message_send = ('\n').join(message_send) 
        
        send_discord_message(message_send, ch='differences')

    return 1




def send_discord_message(message, ch='values'):
    if ch=='values':
        WEBHOOK_URL = config.WEBHOOK_VALUES
    elif ch=='differences':
        WEBHOOK_URL = config.WEBHOOK_DIFFERENCES
    payload = {
                "username": "ARK Tracker",
                "content": message
            }
    requests.post(WEBHOOK_URL, json=payload)


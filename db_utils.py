import pymysql
import os
import pandas as pd

MYSQL_HN = os.environ['MYSQL_HN']
MYSQL_DB = os.environ['MYSQL_DB']
MYSQL_UN = os.environ['MYSQL_UN']
MYSQL_PW = os.environ['MYSQL_PW']

def resolve_symbols(df_scrape):

    conn = pymysql.connect(host=MYSQL_HN, db=MYSQL_DB, user=MYSQL_UN, password=MYSQL_PW)
    cur = conn.cursor()

    symbol_list = []
    cur.execute('select symbol from stock_symbols;')
    for row in cur:
        symbol_list.append(row[0])
    
    df_db = pd.DataFrame({'symbol': symbol_list})

    df_diff = df_scrape[~df_scrape['symbol'].isin(df_db['symbol'])]
    df_diff = df_diff[~df_diff.duplicated('symbol')]

    for i in range(len(df_diff.index)):
        cmd  = 'insert into stock_symbols values ("' + df_diff['symbol'].iloc[i] + '","' + df_diff['name'].iloc[i] + '","' + df_diff['industry'].iloc[i] + '");'
        cur.execute(cmd)

    conn.commit()
    conn.close()

    return len(df_diff.index)

def insert_options(df_options, rec_date):

    conn = pymysql.connect(host=MYSQL_HN, db=MYSQL_DB, user=MYSQL_UN, password=MYSQL_PW)
    cur = conn.cursor()

    rec_date = rec_date.strftime('%Y-%m-%d')

    for i in range(len(df_options.index)):
        contract = df_options['contract'].iloc[i]
        symbol = df_options['symbol'].iloc[i]
        type_ = str(df_options['type'].iloc[i])
        strike = str(df_options['strike'].iloc[i])
        exp_date = df_options['exp_date'].iloc[i].strftime('%Y-%m-%d')
        bid = str(df_options['bid'].iloc[i])
        ask = str(df_options['ask'].iloc[i])
        volume = str(df_options['volume'].iloc[i])

        cmd = 'insert into option_data values ("' + contract + '","' + rec_date + '","' + symbol + '",'
        cmd = cmd + type_ + ',' + strike + ',"' + exp_date + '",' + bid + ',' + ask + ',' + volume + ',NULL);'
        cur.execute(cmd)

    conn.commit()
    conn.close()

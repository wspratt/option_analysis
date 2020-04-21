import pymysql
import os
import pandas as pd

MYSQL_HN = os.environ['MYSQL_HN']
MYSQL_DB = os.environ['MYSQL_DB']
MYSQL_UN = os.environ['MYSQL_UN']
MYSQL_PW = os.environ['MYSQL_PW']

def get_connection():

    conn = pymysql.connect(host=MYSQL_HN, db=MYSQL_DB, user=MYSQL_UN, password=MYSQL_PW)
    cur = conn.cursor()

    return [conn, cur]

def resolve_symbols(df_scrape):
    
    [conn, cur] = get_connection()

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

    [conn, cur] = get_connection()

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

def get_symbol_list():

    [conn, cur] = get_connection()

    symbol_list = []
    cur.execute('select symbol from stock_symbols;')
    for row in cur:
        symbol_list.append(row[0])

    conn.close()

    return symbol_list

def get_unvalued_options():

    [conn, cur] = get_connection()

    symbol_list = []
    date_list = []

    cur.execute('select distinct symbol, rec_date from option_data where est_val is null order by rec_date asc;')
    for row in cur:
        symbol_list.append(row[0])
        date_list.append(row[1])

    conn.close()

    return pd.DataFrame({'symbol': symbol_list, 'rec_date': date_list})
    
def insert_historical_data(symbol, df_close, df_div, df_ss):

    [conn, cur] = get_connection()

    for i in range(len(df_close.index)):
        cmd = 'insert into historical_data values("' + symbol + '","' + df_close['rec_date'].iloc[i].strftime('%Y-%m-%d') + '",' + str(df_close['close'].iloc[i]) + ');'
        cur.execute(cmd)
        conn.commit()

    for i in range(len(df_div.index)):
        cmd = 'insert into dividend_data values("' + symbol + '","' + df_div['rec_date'].iloc[i].strftime('%Y-%m-%d') + '",' + str(df_div['dividend'].iloc[i]) + ');'
        cur.execute(cmd)
        conn.commit()

    for i in range(len(df_ss.index)):
        cmd = 'insert into stocksplit_data values("' + symbol + '","' + df_ss['rec_date'].iloc[i].strftime('%Y-%m-%d') + '",' + str(df_ss['stocksplit'].iloc[i]) + ');'
        cur.execute(cmd)
        conn.commit()

    conn.close()

def get_latest_historical_date(symbol):

    [conn, cur] = get_connection()

    qty = cur.execute('select rec_date from historical_data where symbol = "' + symbol + '" order by rec_date desc limit 0,1')
    if qty == 0:
        return None

    return cur.fetchone()[0]

def get_historical_data(symbol, rec_date, lookback):

    [conn, cur] = get_connection()

    date_arr = []
    close_arr = []

    cmd = 'select rec_date, close from historical_data where symbol = "' + symbol + '" and rec_date <= "' + rec_date.strftime('%Y-%m-%d') + '" order by rec_date desc limit 0,' + str(lookback) + ';'
    cur.execute(cmd)

    for line in cur:
        date_arr.append(line[0])
        close_arr.append(line[1])

    df_close = pd.DataFrame({'rec_date': date_arr, 'close': close_arr})

    date_arr = []
    div_arr = []

    cmd = 'select rec_date, dividend from dividend_data where symbol = "' + symbol + '" and rec_date <= "' + rec_date.strftime('%Y-%m-%d') + '" and rec_date >= "' + df_close['rec_date'].iloc[-1].strftime('%Y-%m-%d') + '" order by rec_date desc;'
    cur.execute(cmd)

    for line in cur:
        date_arr.append(line[0])
        div_arr.append(line[1])

    df_div = pd.DataFrame({'rec_date': date_arr, 'dividend': div_arr})

    date_arr = []
    ss_arr = []

    cmd = 'select rec_date, stocksplit from stocksplit_data where symbol = "' + symbol + '" and rec_date <= "' + rec_date.strftime('%Y-%m-%d') + '" and rec_date >= "' + df_close['rec_date'].iloc[-1].strftime('%Y-%m-%d') + '" order by rec_date desc;'
    cur.execute(cmd)

    for line in cur:
        date_arr.append(line[0])
        ss_arr.append(line[1])

    df_ss = pd.DataFrame({'rec_date': date_arr, 'stocksplit': ss_arr})

    conn.close()

    return [df_close, df_div, df_ss]

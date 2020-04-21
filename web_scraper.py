from urllib import error
from urllib import request
import pandas as pd
import time
import datetime
import sys

def try_connect(url):
    for i in range(5):
        try:
            resp = request.urlopen(url)
            text = resp.read().decode('utf-8')
            return text
        except:
            time.sleep(1)
    return None

def str2date(string):
    date1 = datetime.date(2018, 1, 1)
    string1 = 1514782800

    return date1 + datetime.timedelta(days=int((int(string) - string1)/(24*60*60)) + 1)

def date2str(date):
    date1 = datetime.date(2018, 1, 1)
    string1 = 1514764800

    day_delta = int((date - date1).days)
    return int(string1 + day_delta*24*60*60)

def scrape_symbols():

    url = r'http://finance.yahoo.com/industries'

    text = try_connect(url)

    text = text.split('"name":"Industries"')[1]
    line_arr = text.split('{')
    line_arr.pop(0)

    industry_name = []
    industry_url = []

    for line in line_arr:
        if 'children' in line:
            break
        text_name = line.split('"')[3]
        url_name = line.split('"')[7].split('F')[-1]
        industry_name.append(text_name)
        industry_url.append(url_name)

    industry_count = []

    base_url = r'http://finance.yahoo.com/sector/'
    for i in range(len(industry_url)):
        url = base_url + industry_url[i]
        text = try_connect(url)
        stock_count = int(text.split('class="Mstart(15px) Fw(500) Fz(s) Mstart(0)--mobp Fl(start)--mobp"')[1].split('results')[0].split('of')[-1].strip())
        industry_count.append(stock_count)

    df_industry = pd.DataFrame({'name': industry_name, 'url': industry_url, 'count': industry_count})

    symbol_list = []
    name_list = []
    industry_list = []

    for i in range(len(df_industry.index)):
        industry_url = base_url + df_industry['url'].iloc[i]
        page_iter = int(df_industry['count'].iloc[i]/100)
        for j in range(0, page_iter + 1):
            url = industry_url + '?offset=' + str(j*100) + '&count=100'
            text = try_connect(url)
            line_arr = text.split('fin-scr-res-table')[1].split('<tbody')[1].split('</tbody')[0].split('<tr')
            line_arr.pop(0)
            for line in line_arr:
                c_name = line.split('title="')[1].split('"')[0]
                c_name = c_name.replace('"', "'")
                c_name = c_name.replace(r'&#x27;', "'")
                c_name = c_name.replace(r'&amp;', r'&')
                c_sym = line.split('</a>')[0].split('>')[-1]
                symbol_list.append(c_sym)
                name_list.append(c_name)
                industry_list.append(df_industry['name'].iloc[i])

    df_symbols = pd.DataFrame({'symbol': symbol_list, 'name': name_list, 'industry': industry_list})
    return df_symbols

def brute_force(method, args, column):

    df = method(args)
    df_len = len(df.index)
    n = 0
    
    while n < 2:
        df_new = method(args)
        df = pd.concat([df, df_new]).drop_duplicates(subset=column).reset_index(drop=True)
        if df_len == len(df.index):
            n = n + 1
        else:
            df_len = len(df.index)
            n = 0

    return df


def try_exp_dates(args):

    symbol = args['symbol']
    rec_date = args['rec_date']
    stock_url = 'https://finance.yahoo.com/quote/' + symbol + '/options?p=' + symbol
    text = try_connect(stock_url)

    if 'expirationDates":[' not in text:
        return pd.DataFrame({'exp_date': []})
    exp_dates = text.split('expirationDates":[')[1].split(']')[0].split(',')
    if len(exp_dates) == 1 and exp_dates[0] == '':
        return pd.DataFrame({'exp_date': []})
    df = pd.DataFrame({'exp_date': exp_dates})
    df['exp_date'] = df['exp_date'].apply(lambda x: str2date(x))
    df['exp_date'] = df['exp_date'][df['exp_date'] < rec_date + datetime.timedelta(days=60)]
    df = df.dropna()
    return df


def try_contracts(args):

    symbol = args['symbol']
    exp_date = args['exp_date']
    rec_date = args['rec_date']

    contract_list = []
    exp_list = []
    type_list = []
    strike_list = []
    bid_list = []
    ask_list = []
    volume_list = []

    ed = date2str(exp_date)

    exp_url = 'https://finance.yahoo.com/quote/' + symbol + '/options?p=' + symbol + '&date=' + str(ed)
    text = try_connect(exp_url)

    if 'table class="calls' in text:
        calls = text.split('table class="calls')[1].split('</tbody')[0].split('<tr')
        calls.pop(0)
        calls.pop(0)

        for row in calls:
            row_arr = row.split('<td')
            contract = row_arr[1].split('</a')[0].split('"')[-1][1:]
            strike = float(row_arr[3].split('</a')[0].split('>')[-1].replace(',',''))
            try:
                bid = float(row_arr[5].split('</td')[0].split('>')[-1].replace(',',''))
            except ValueError:
                bid = 'NULL'
            try:
                ask = float(row_arr[6].split('</td')[0].split('>')[-1].replace(',',''))
            except ValueError:
                ask = 'NULL'
            try:
                volume = int(row_arr[9].split('</td>')[0].split('>')[-1].replace(',',''))
            except ValueError:
                volume = 0

            contract_list.append(contract)
            exp_list.append(str2date(ed))
            type_list.append(0)
            strike_list.append(strike)
            bid_list.append(bid)
            ask_list.append(ask)
            volume_list.append(volume)

    if 'table class="puts' in text:
        puts = text.split('table class="puts')[1].split('</tbody')[0].split('<tr')
        puts.pop(0)
        puts.pop(0)

        for row in puts:
            row_arr = row.split('<td')
            contract = row_arr[1].split('</a')[0].split('"')[-1][1:]
            strike = float(row_arr[3].split('</a')[0].split('>')[-1].replace(',',''))
            try:
                bid = float(row_arr[5].split('</td')[0].split('>')[-1].replace(',',''))
            except ValueError:
                bid = 'NULL'
            try:
                ask = float(row_arr[6].split('</td')[0].split('>')[-1].replace(',',''))
            except ValueError:
                ask = 'NULL'
            try:
                volume = int(row_arr[9].split('</td>')[0].split('>')[-1].replace(',',''))
            except ValueError:
                volume = 0

            contract_list.append(contract)
            exp_list.append(str2date(ed))
            type_list.append(1)
            strike_list.append(strike)
            bid_list.append(bid)
            ask_list.append(ask)
            volume_list.append(volume)

    df_options = pd.DataFrame({
        'contract': contract_list,
        'exp_date': exp_list,
        'type': type_list,
        'strike': strike_list,
        'bid': bid_list,
        'ask': ask_list,
        'volume': volume_list
        })

    df_options['symbol'] = symbol
    df_options['rec_date'] = rec_date

    return df_options

def scrape_options(symbol, rec_date):

    df_exp_dates = brute_force(try_exp_dates, {'symbol': symbol, 'rec_date': rec_date}, 'exp_date')

    df_options = pd.DataFrame({
        'contract': [],
        'exp_date': [],
        'type': [],
        'strike': [],
        'bid': [],
        'ask': [],
        'volume': []
        })


    for i in range(len(df_exp_dates.index)):
        df_contract = brute_force(try_contracts, {'symbol': symbol, 'exp_date': df_exp_dates['exp_date'].iloc[i], 'rec_date': rec_date}, 'contract')
        df_options = pd.concat([df_options, df_contract]).drop_duplicates(subset='contract').reset_index(drop=True)

    return df_options

def scrape_historical_data(symbol, end_date, lookback):

    end_date = end_date + datetime.timedelta(days=min(1, 5 - end_date.weekday()))

    base_url = r'https://finance.yahoo.com/quote/' + symbol

    rec_date = []
    close = []

    div_date = []
    div_val = []

    ss_date = []
    ss_val = []

    start_date = end_date + datetime.timedelta(days=-lookback)
    start_date = start_date + datetime.timedelta(days=min(1, 5 - start_date.weekday()))

    while True:

        period2 = date2str(end_date)
        temp_date = end_date + datetime.timedelta(days=-100)
        period1 = date2str(temp_date)

        target_url = base_url + '/history?period1=' + str(period1) + '&period2=' + str(period2)
        text = try_connect(target_url)

        line_arr = text.split('data-test="historical-prices"')[1].split('<tbody')[1].split('</tbody')[0].split('<tr')
        line_arr.pop(0)

        for line in line_arr:
            if 'Dividend' in line:
                date_str = line.split('</span')[0].split('>')[-1]
                d_date = datetime.datetime.fromtimestamp(time.mktime(time.strptime(date_str, '%b %d, %Y'))).date()
                d_div = float(line.split('</strong')[0].split('>')[-1])
                div_date.append(d_date)
                div_val.append(d_div)
                continue
            if 'Stock Split' in line:
                date_str = line.split('</span')[0].split('>')[-1]
                d_date = datetime.datetime.fromtimestamp(time.mktime(time.strptime(date_str, '%b %d, %Y'))).date()
                d_ss_arr = line.split('</strong')[0].split('>')[-1].split(':')
                d_ss = float(d_ss_arr[0])/float(d_ss_arr[1])
                ss_date.append(d_date)
                ss_val.append(d_ss)
                continue
            span_arr = []
            for span in line.split('</span'):
                data = span.split('>')[-1]
                span_arr.append(data)

            d_date = datetime.datetime.fromtimestamp(time.mktime(time.strptime(span_arr[0], '%b %d, %Y'))).date()
            d_close = float(span_arr[4].replace(',',''))

            rec_date.append(d_date)
            close.append(d_close)

            if d_date <= start_date:
                break

        if d_date <= start_date:
            break

        end_date = rec_date[-1]


    df_close = pd.DataFrame({'rec_date': rec_date, 'close': close})
    df_div = pd.DataFrame({'rec_date': div_date, 'dividend': div_val})
    df_ss = pd.DataFrame({'rec_date': ss_date, 'stocksplit': ss_date})
    
    return df_close, df_div, df_ss

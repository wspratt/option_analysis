from urllib import request
import datetime
import sys
import pandas as pd

def scrape_symbols():

    url = r'http://finance.yahoo.com/industries'

    resp = request.urlopen(url)
    text = resp.read().decode('utf-8')

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
        resp = request.urlopen(url)
        text = resp.read().decode('utf-8')
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
            resp = request.urlopen(url)
            text = resp.read().decode('utf-8')
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

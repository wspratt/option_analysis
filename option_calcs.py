import datetime
import db_utils
import numpy
import web_scraper

def calculate_adjusted_close(df_close, df_div, df_ss):

    multiplier = 1.0
    adj_close = []

    for rd in df_close.index:
        adj_close.append(round(df_close['close'][rd]*multiplier,2))
        if rd in df_div.index:
            multiplier = multiplier * (df_close['close'][rd] - df_div['dividend'][rd])/df_close['close'][rd]
        if rd in df_ss.index:
            multiplier = multiplier * df_ss['stocksplit'][rd]

    df_close['adj_close'] = adj_close
    return df_close

def ewma_vol(df_close):

    A = 0.90

    volatility = [numpy.sqrt(numpy.log(df_close['adj_close'].iloc[-2]/df_close['adj_close'].iloc[-1])**2)*numpy.sqrt(252), numpy.nan]

    for i in range(len(df_close.index) - 3, -1, -1):
        volatility.insert(0, numpy.sqrt(A*volatility[0]**2+(1-A)*(numpy.log(df_close['adj_close'].iloc[i]/df_close['adj_close'].iloc[i+1])*numpy.sqrt(252))**2))

    return volatility[0]

def get_interest_rate(rec_date):

    rate = db_utils.get_interest_rate(rec_date)

    if rate is None:
        rate = web_scraper.scrape_interest_rate(rec_date)
        db_utils.insert_interest_rate(rec_date, rate)

    return rate

def calculate_volatility(symbol, rec_date):

    latest_date = db_utils.get_latest_historical_date(symbol)

    if latest_date is None:
        [df_close, df_div, df_ss] = web_scraper.scrape_historical_data(symbol, rec_date, 365)
        db_utils.insert_historical_data(symbol, df_close, df_div, df_ss)

    elif latest_date < rec_date:        
        next_date = latest_date + datetime.timedelta(days=1)
        if next_date.weekday() > 4:
            next_date = next_date + datetime.timedelta(days=7 - next_date.weekday())

        lookback = (rec_date - next_date).days
        [df_close, df_div, df_ss] = web_scraper.scrape_historical_data(symbol, rec_date, lookback)
        db_utils.insert_historical_data(symbol, df_close, df_div, df_ss)

    latest_date = db_utils.get_latest_historical_date(symbol)
    
    if latest_date < rec_date:
        return None

    [df_close, df_div, df_ss] = db_utils.get_historical_data(symbol, rec_date, 130)
    df_close = calculate_adjusted_close(df_close, df_div, df_ss)
    return ewma_vol(df_close)
    

        

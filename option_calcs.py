import datetime
import db_utils
import numpy
import pandas as pd
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
        historical_data = web_scraper.scrape_historical_data(symbol, rec_date, 365)
        if historical_data is None:
            return None
        [df_close, df_div, df_ss] = historical_data
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

def predict_next_dividend(symbol, rec_date):

#    print('attempting to predict next dividend for ' + symbol + ' starting from ' + rec_date.strftime('%Y-%m-%d'))
#    input()
    latest_date = db_utils.get_latest_historical_date(symbol)
#    print('the latest available date for ' + symbol + ' is ' + latest_date.strftime('%Y-%m-%d'))
#    input()

    if latest_date > rec_date:
#        print(latest_date.strftime('%Y-%m-%d') + ' is later than ' + rec_date.strftime('%Y-%m-%d') + ', so first look for a dividend in the database')
#        input()
        delta = (latest_date - rec_date).days
        [df_close, df_div, df_ss] = db_utils.get_historical_data(symbol, latest_date, delta)
        df_div = df_div[df_div['rec_date'] > rec_date]
        if len(df_div.index) > 0:
#            print('dividend found in the database')
#            input()
            return df_div.tail(1)
#        else:
#            print('no dividend was found in the database. proceeding to extrapolation')
#            input()
#    else:
        
#        print(latest_date.strftime('%Y-%m-%d') + ' is not later than ' + rec_date.strftime('%Y-%m-%d') + ', proceeding to extrapolation')
#        input()

    [df_close, df_div, df_ss] = db_utils.get_historical_data(symbol, latest_date, 252)
    if len(df_div.index) == 0:
#        print(symbol + ' paid no dividends in the last year, so no future dividends are predicted')
#        input()
        return None
    else:
        dividend_frequency = len(df_div.index)
#        print(symbol + ' paid ' + str(dividend_frequency) + ' dividend(s) in the last year:')
#        input()
        df_div['delta'] = (df_div['rec_date'] - df_div['rec_date'].shift(-1))
        df_div['delta'] = df_div['delta'].apply(lambda x: x.days)
#        print(df_div)
#        input()
        calculated_delta = int(df_div['delta'].mean())
        next_expected_date = df_div['rec_date'].iloc[0] + datetime.timedelta(days=calculated_delta)
        next_expected_date = next_expected_date + datetime.timedelta(days=max(0, next_expected_date.weekday() - 4))
        last_dividend = df_div['dividend'].iloc[0]
#        print('based on an average delta of ' + str(calculated_delta) + ' days, the next dividend is expected to be paid on ' + next_expected_date.strftime('%Y-%m-%d'))
#        input()        
        
        df_div = web_scraper.scrape_dividend(symbol)
        if df_div is None:
#            print('dividend not read successfully from web_scrape, using database prediction')
#            input()
            df_div = pd.DataFrame({'rec_date': [next_expected_date], 'dividend': [last_dividend]})
            return df_div
        else:
#            print('web scrape yields dividend of ' + str(df_div['dividend'].iloc[0]) + '/' + str(dividend_frequency) + ' = ' + str(round(df_div['dividend'].iloc[0]/dividend_frequency, 2)) + ' on ' + df_div['rec_date'].iloc[0].strftime('%Y-%m-%d'))
#            input()

            total_dividend = df_div['dividend'].iloc[0]
            next_expected_dividend = round(total_dividend/dividend_frequency,2)

            if df_div['rec_date'].iloc[0] > latest_date:
#                print('web scrape dividend date is later than last historical date, using web scrape date')
#                input()
                df_div = pd.DataFrame({'rec_date': [df_div['rec_date'].iloc[0]], 'dividend': [next_expected_dividend]})
                return df_div
            else:
#                print('web scrape dividend date is earlier than last historical date, using database prediction date')
#                input()
                df_div = pd.DataFrame({'rec_date': [next_expected_date], 'dividend': [next_expected_dividend]})
                return df_div

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

def get_volatility(symbol, rec_date):

    vol = db_utils.get_volatility(symbol, rec_date)
    if vol is None:
        vol = calculate_volatility(symbol, rec_date)
        if vol is not None:
            vol = float(vol)
            db_utils.insert_volatility(symbol, rec_date, vol)
    else:
        vol = float(vol)
        
    return vol

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

def compute_option(o_type, stock, strike, vol, r, T, div, div_T, steps):

    dt = T/steps

    u = numpy.exp(vol*numpy.sqrt(dt))
    d = numpy.exp(-vol*numpy.sqrt(dt))
    a = numpy.exp(r*dt)
    p = ((a - d)/(u - d))

    div_disc = []
    for i in range(0, steps + 1):
        if i*dt < div_T:
            div_disc.append(round(div*numpy.exp(-r*(div_T - i*dt)),2))
        else:
            div_disc.append(0)

    S = [[stock]]
    D = [[stock + div_disc[0]]]
    V = [[0]]

    for i in range(1, steps + 1):
        new_row = []
        new_dis = []
        new_val = []
        for j in range(0, i+1):
            new_row.append(round(stock*(u**j)*(d**(i-j)),2))
            new_dis.append(round(new_row[-1] + div_disc[i], 2))
            new_val.append(0)
        S.append(new_row)
        D.append(new_dis)
        V.append(new_val)

    for i in range(0, len(S[-1])):
        if o_type == 0:
            V[-1][i] = round(max(D[-1][i] - strike, 0), 2)
        elif o_type == 1:
            V[-1][i] = round(max(strike - D[-1][i], 0), 2)

    for i in range(len(S) - 2, -1, -1):
        for j in range(0, i + 1):
            if o_type == 0:
                V[i][j] = round(max(D[i][j] - strike, (p*V[i+1][j+1]+(1-p)*V[i+1][j])*numpy.exp(-r*dt)),2)
            elif o_type == 1:
                V[i][j] = round(max(strike - D[i][j], (p*V[i+1][j+1]+(1-p)*V[i+1][j])*numpy.exp(-r*dt)),2)

    return V[0][0]

def eval_option(o_type, stock, strike, vol, r, rec_date, exp_date, div_val, div_date):

    T = (exp_date - rec_date).days/365.0

    div_T = 0.0
    if div_date is not None:
        div_T = (div_date - rec_date).days/365.0

    if div_T > T:
        div_T = 0.0
        div_val = None

    if div_val is None:
        div_val = 0.0

    steps = 30
    fval = compute_option(o_type, stock, strike, vol, r, T, div_val, div_T, steps)
    return fval
    
#    while True:
#        fval_next = compute_option(o_type, stock, strike, vol, r, T, div_val, div_T, steps + 1)
#        if fval_next == fval:
#            break
#        fval = fval_next
#        steps = steps + 1

#    return fval

def bulk_eval_options(symbol, rec_date):

    df_options = db_utils.get_unvalued_contracts(symbol, rec_date)
    vol = get_volatility(symbol, rec_date)
    [df_close, df_div, df_ss] = db_utils.get_historical_data(symbol, rec_date, 1)
    stock = df_close['close'].iloc[0]
    r = get_interest_rate(rec_date)
    df_div = predict_next_dividend(symbol, rec_date)
    if df_div is None:
        div_val = None
        div_date = None
    else:
        div_val = df_div['dividend'].iloc[0]
        div_date = df_div['rec_date'].iloc[0]

    val_list = []
    for i in range(len(df_options.index)):
        val_list.append(eval_option(int(df_options['type'].iloc[i]), float(stock), float(df_options['strike'].iloc[i]), float(vol), float(r), rec_date, df_options['exp_date'].iloc[i], div_val, div_date))

    df_options['est_val'] = val_list

    return df_options

def get_pareto_set(rec_date, volume_min=1000, ask_max=100, day_min=30, ret_max=100):

    min_exp = rec_date + datetime.timedelta(days=day_min)
    df_options = db_utils.get_generic_df('contract,strike,exp_date,ask,est_val,volume','from option_data where est_val != 0.0 and volume > ' + str(volume_min) + ' and ask < ' + str(ask_max) + ' and exp_date > "' + min_exp.strftime('%Y-%m-%d') + '" and rec_date = "' + rec_date.strftime('%Y-%m-%d') + '";')
    r = get_interest_rate(rec_date)
    def compute_ret(x):
        days = (x['exp_date'] - rec_date).days
        try:
            if x['ask'] > 0:
                return round(float(x['est_val']/x['ask'])**(365.0/days) - 1 - ((1+r)**(days/30.0) - 1), 2)
            else:
                return 0
        except:
            return 0

    df_options['ret'] = df_options.apply(lambda x: compute_ret(x), axis=1)
    df_options = df_options[(df_options['ret'] > 0) & (df_options['ret'] < ret_max)]
    
    df_pareto = df_options.copy()
    df_pareto = df_pareto[0:0]
    for i in range(len(df_options.index)):
        df_dominating = df_options[(df_options['volume'] > df_options['volume'].iloc[i]) & (df_options['ret'] > df_options['ret'].iloc[i])]
        if len(df_dominating.index) == 0:
            df_pareto = df_pareto.append(df_options.iloc[i])
    df_pareto = df_pareto.sort_values('volume')

    return [df_pareto, df_options]


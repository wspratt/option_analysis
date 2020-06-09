import web_scraper
import option_calcs
import db_utils
import datetime
import sys

def tprint(msg):
    print('[' + str(datetime.datetime.now()) + '] ' + msg)
    sys.stdout.flush()

restart = False
eval_flag = False

df_symbols = None

if len(sys.argv) == 2:
    if sys.argv[1] == 'restart':
        restart = True
    elif sys.argv[1] == 'eval':
        eval_flag = True

if restart is False and eval_flag is False:

    rec_date = datetime.datetime.today().date()

    tprint('starting download process for ' + rec_date.strftime('%Y-%m-%d') + '.')
    tprint('scraping symbols from industry list...')

    df_scrape = web_scraper.scrape_symbols()
    new_count = str(db_utils.resolve_symbols(df_scrape))

    tprint(new_count + ' new symbol(s) added to database to track.')

    tprint('beginning option download...')
    df_symbols = db_utils.get_symbol_list()

elif restart is True:

    [rec_date, df_symbols] = db_utils.get_option_restart_point()
    tprint('restarting option download at symbol ' + df_symbols[0])

if df_symbols is not None:
    for i in range(len(df_symbols)):
        df_options = web_scraper.scrape_options(df_symbols[i], rec_date)    
        if df_options is not None:
            db_utils.insert_options(df_options, rec_date)
            tprint('[ODL] [' + str(i + 1) + '/' + str(len(df_symbols)) + '] [' + df_symbols[i] + '] [' + str(len(df_options.index)) + ']')
        else:
            tprint('[ODL] [' + str(i + 1) + '/' + str(len(df_symbols)) + '] [' + df_symbols[i] + '] [NULL]')

tprint('proceeding to options valuation')

df_options = db_utils.get_unvalued_options()

tprint(str(len(df_options)) + ' symbol-date pairs queued up to value.')

for i in range(len(df_options)):
    try:
        df_contracts = option_calcs.bulk_eval_options(df_options['symbol'].iloc[i], df_options['rec_date'].iloc[i])
        db_utils.insert_valued_options(df_contracts, df_options['rec_date'].iloc[i])
        num_results = len(df_contracts.index)
    except:
        num_results = 0
    tprint('[OVAL] [' + str(i+1) + '/' + str(len(df_options)) + '] [' + df_options['symbol'].iloc[i] + '] [' + df_options['rec_date'].iloc[i].strftime('%Y-%m-%d') + '] [' + str(num_results) + ']')

tprint('purging old unvalued options from database')

qty = db_utils.drop_old_unvalued_options()

tprint(str(qty) + ' contracts dropped')

tprint('daily script complete.')



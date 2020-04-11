import web_scraper
import db_utils
import datetime
import sys

def tprint(msg):
    print('[' + str(datetime.datetime.now()) + '] ' + msg)
    sys.stdout.flush()

rec_date = datetime.date(2020, 4, 9)

tprint('starting download process for ' + rec_date.strftime('%Y-%m-%d') + '.')
tprint('scraping symbols from industry list...')

df_scrape = web_scraper.scrape_symbols()
new_count = str(db_utils.resolve_symbols(df_scrape))

tprint(new_count + ' new symbol(s) added to database to track.')

tprint('beginning option download...')
df_symbols = db_utils.get_symbol_list()

for i in range(len(df_symbols)):
    df_options = web_scraper.scrape_options(df_symbols[i], rec_date)    
    if df_options is not None:
        db_utils.insert_options(df_options, rec_date)
        tprint('[ODL] [' + str(i + 1) + '/' + str(len(df_symbols)) + '] [' + df_symbols[i] + '] [' + str(len(df_options.index)) + ']')
    else:
        tprint('[ODL] [' + str(i + 1) + '/' + str(len(df_symbols)) + '] [' + df_symbols[i] + '] [NULL]')



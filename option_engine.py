import web_scraper
import db_utils
import datetime
import sys

def tprint(msg):
    print('[' + str(datetime.datetime.now()) + '] ' + msg)
    sys.stdout.flush()

rec_date = datetime.date(2020, 4, 3)

tprint('starting download process for ' + rec_date.strftime('%Y-%m-%d') + '.')
tprint('scraping symbols from industry list...')

df_symbols = web_scraper.scrape_symbols()
new_count = str(db_utils.resolve_symbols(df_symbols))

tprint(new_count + ' new symbol(s) added to database to track.')

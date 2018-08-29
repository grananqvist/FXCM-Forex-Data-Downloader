import os
import sys
import fxcmpy
from tqdm import tqdm
import math
import argparse
from datetime import datetime, timedelta

LARGE_TFS = ['D1', 'W1', 'M1']
MEDIUM_TFS = ['H1', 'H2', 'H3', 'H4', 'H8']
SMALL_TFS = ['m1', 'm5', 'm15', 'm30']
STEPS = { 
    **{ k: timedelta(weeks=52*10) for k in LARGE_TFS },
    **{ k: timedelta(weeks=52) for k in MEDIUM_TFS },
    **{ k: timedelta(weeks=1) for k in SMALL_TFS }
}

def download(period, symbol, token, path='./'):
    """ Downloads forex and index cfd historical data from FXCM API 
    during the period 2000-01-01 to todays date

    Arguments:
    period - the timeframe to use
    symbol - a list of symbols to download
    token - required FXCM API token
    path - path for saving the data on disk
    """

    con = fxcmpy.fxcmpy(access_token=token, log_level='error')
    
    all_symbols = con.get_instruments()
    symbols = symbol if symbol is not None else all_symbols
    print('All symbols: ',all_symbols)
    print('Symbols to download: ',symbols)

    # download for all symbols
    for symbol in symbols:

        start_date = datetime(2000,1,1) 
        end_date = start_date + STEPS[period]

        header = True
        with open(os.path.join(path, symbol.replace('/','') + '_' + period + '.csv'), 'a') as f:

            # split download up in chunks of size `STEPS`

            num_steps = math.ceil( (datetime.now() - start_date) / STEPS[period] )
            for _ in tqdm(range(num_steps)):

                # fetch data 
                df = con.get_candles(symbol, period=period,
                        start=start_date, end=end_date)

                # append to csv
                df.to_csv(f, header=header)

                start_date = end_date 
                if period not in LARGE_TFS:
                    start_date += timedelta(minutes=1)

                end_date += STEPS[period]
                header = False


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='FXCM Historical data downloader')
    parser.add_argument('-t', '--token', help='Your FXCM API token to authorize for access. Works with a demo account key', default=None)
    parser.add_argument('-s', '--symbol', nargs='+', help='symbols to download. Flag can be used to download multiple symbols. Example: -s EUR/USD AUD/USD. Downloads all symbols by default', default=None)
    parser.add_argument('-pe', '--period', help='Time frame for data to download. m1, m5, m15, m30, H1, H2, H3, H4, H6, H8, D1, W1, M1', default='m1')
    parser.add_argument('-p', '--path', help='Path to store downloaded data in', default='./')
    params = parser.parse_args(sys.argv[1:])

    assert params.token is not None, "you must provide an FXCM API token. They are free to get using a demo account"

    download(**vars(params))

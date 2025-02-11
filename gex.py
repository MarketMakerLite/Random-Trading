import datetime
from sqlalchemy import create_engine, text
import pandas as pd
import config
import concurrent.futures
import itertools
import time
import traceback
import requests

engine = create_engine(config.psql, pool_size=10, max_overflow=20)


def get_symbols(engine):
    symbol_df = pd.read_sql_query('select * from companies where market_cap >= 3000000000', con=engine).sort_values("market_cap", ascending=False)
    symbols = symbol_df['ticker'].tolist()
    return symbols


def main(symbol):
    print(f'getting data for: {symbol}')
    try:
        # Get options data
        df = pd.read_sql_query(f"select uticker, \"putCall\", \"quoteTimeInLong\", \"openInterest\", \"totalVolume\", \"strikePrice\", gamma, tdate "
                               f"from optionsdata where uticker = '{symbol}' and tdate = (select max(tdate)"
                               f" from optionsdata where uticker = '{symbol}')", con=engine)

        # Get Spot Price
        spot_price_df = pd.read_sql_query(f"select symbol, tick_close, tdate from stockdata_hist where symbol = '{symbol}'"
                                             f" and tdate = (select max(tdate) from stockdata_hist"
                                             f" where symbol = '{symbol}' AND (CAST(tdate AS TIME) between '13:30' and '20:00'))", con=engine)

        spot_price = spot_price_df['tick_close'][0]

        # Calculate GEX
        calls_df = df.loc[df['putCall'] == 'CALL'].reset_index()
        puts_df = df.loc[df['putCall'] == 'PUT'].reset_index()

        # GEX(shares)
        df_shares = df
        for x in df['putCall']:
            if x == 'CALL':
                df_shares['gex'] = df['openInterest'] * df['gamma'] * 100
            if x == 'PUT':
                df_shares['gex'] = df['openInterest'] * df['gamma'] * -100

        gex_shares = df_shares['gex'].sum()

        # GEX($)
        df_dollars = df
        for x in df['putCall']:
            if x == 'CALL':
                df_dollars['gex'] = df['openInterest'] * df['gamma'] * 100 * spot_price
            if x == 'PUT':
                df_dollars['gex'] = df['openInterest'] * df['gamma'] * -100 * spot_price
        gex_dollars = df_dollars['gex'].sum()

        # GEX($) per 1% move
        df_dollars_adj = df
        for x in df['putCall']:
            if x == 'CALL':
                df_dollars_adj['gex'] = df['openInterest'] * df['gamma'] * 100 * spot_price * (0.01 * spot_price)
            if x == 'PUT':
                df_dollars_adj['gex'] = df['openInterest'] * df['gamma'] * -100 * spot_price * (0.01 * spot_price)
        gex_dollars_adj = df_dollars_adj['gex'].sum()

        # Put Call Ratio calculation
        if calls_df['totalVolume'].sum() == 0:
            pcr = 0
        else:
            pcr = puts_df['totalVolume'].sum() / calls_df['totalVolume'].sum()

        # Calculate flip-point
        flip_point_df = df_shares[['strikePrice', 'gex']]
        strikes = flip_point_df.to_records(index=False)

        def aux_add(a, b): return (b[0], a[1] + b[1])

        cumsum = list(itertools.accumulate(strikes, aux_add))
        if cumsum[len(strikes) // 10][1] < 0:
            op = min
        else:
            op = max
        flip_point = op(cumsum, key=lambda i: i[1])[0]

        # Combine calculations into a dataframe
        values_dict = {'uticker': symbol, 'gex_shares': gex_shares, 'gex_dollars': gex_dollars, 'gex_dollars_adj': gex_dollars_adj,
                       'pcr': pcr, 'flip_point': flip_point, 'tdate': df['tdate'][0], 'save_date': datetime.datetime.now(datetime.timezone.utc)}
        results_df = pd.DataFrame(values_dict, index=[0])
    except Exception as exe:
        print(exe, symbol)
        #traceback.print_exc()
        results_df = pd.DataFrame()
        pass
    return results_df


def index_calc(engine):
    # Get SPX components
    df = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
    df = pd.DataFrame(df[0:][0])
    spx = df.Symbol.to_list()
    spx = tuple(spx)
    print(spx)
    print(len(spx))

    # Query DB for SPX components
    spx_df = pd.read_sql_query(f" SELECT * "
                               f"FROM gex a "
                               f"JOIN (SELECT uticker, max(save_date) maxDate FROM gex GROUP BY uticker) b "
                               f"ON a.uticker = b.uticker AND a.save_date = b.maxDate AND a.uticker IN {spx};",
                               con=engine)
    spx_df = spx_df.iloc[:, :8]
    print(spx_df)

    # Get SPX Weights
    url = 'https://www.slickcharts.com/sp500'
    hdr = {
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11'}
    html = requests.get(url, headers=hdr).content
    df = pd.read_html(html)[0]
    weights = df[['Symbol', 'Weight']].reset_index(drop=True)
    weights.loc[:, 'Weight'] = weights['Weight'].apply(lambda x: x / 100)
    weights.rename(columns={'Symbol': 'uticker', 'Weight': 'weight'}, inplace=True)

    # Sum Values and apply weights
    spx_gex = pd.merge(spx_df[['uticker', 'gex_shares', 'gex_dollars', 'gex_dollars_adj', 'pcr', 'flip_point']],
                       weights, on='uticker', how='inner')

    spx_gex['gex_shares_weighted'] = spx_gex['gex_shares'] * spx_gex['weight']
    spx_gex['gex_dollars_weighted'] = spx_gex['gex_dollars'] * spx_gex['weight']
    spx_gex['gex_dollars_adj_weighted'] = spx_gex['gex_dollars_adj'] * spx_gex['weight']

    gex_shares = spx_gex['gex_shares_weighted'].sum()
    gex_dollars = spx_gex['gex_dollars_weighted'].sum()
    gex_dollars_adj = spx_gex['gex_dollars_adj_weighted'].sum()

    values_dict = {'uticker': 'SPX', 'gex_shares': gex_shares, 'gex_dollars': gex_dollars,
                   'gex_dollars_adj': gex_dollars_adj,
                   'pcr': None, 'flip_point': None, 'tdate': spx_df['tdate'][0],
                   'save_date': datetime.datetime.now(datetime.timezone.utc)}
    df = pd.DataFrame(values_dict, index=[0])
    dbwrite(df)
    return None


def threadpool(symbols):
    df = pd.DataFrame()
    with concurrent.futures.ThreadPoolExecutor() as executor:
        results = executor.map(main, symbols)
        for result_df in results:
            df = pd.concat([df, result_df])
    # Write to Database
    print('writing to database')
    dbwrite(df)
    return None


def dbwrite(df):
    df.to_sql('gex', engine, if_exists='append', index=False, method='multi', chunksize=100000)
    return None


if __name__ == "__main__":
    try:
        start_time = time.time()
        symbols = get_symbols(engine)
        threadpool(symbols)
        index_calc(engine)
        print((time.time() - start_time)/60)

    except Exception as exe:
        print(exe)
        traceback.print_exc()



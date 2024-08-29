import asyncio
import pickle
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional

import httpx
from tqdm.asyncio import tqdm_asyncio
from tqdm import tqdm

import api_config
import n100

API_KEY = f'apiKey={api_config.POLYGON_API_KEY}'

def date_range(start_date, end_date):
    start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
    end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
    dates = []
    current_date = start_date
    while current_date <= end_date:
        dates.append(current_date.strftime('%Y-%m-%d'))
        current_date += timedelta(days=1)
    return dates

@dataclass
class TickerInfo:
    ticker: str
    exchange: str
    cik: Optional[str]
    type: str
    active: bool
    market_cap: Optional[int]

async def get_ticker_info(client, semaphore, ticker, date):
    async with semaphore:
        resp = await client.get(f'https://api.polygon.io/v3/reference/tickers/{ticker}?date={date}&{API_KEY}')
    resp = resp.json()
    if 'results' not in resp:
        return None
    results = resp['results']
    return TickerInfo(
        results['ticker'],
        results['primary_exchange'],
        results['cik'] if 'cik' in results else None,
        results['type'] if 'type' in results else None,
        results['active'],
        results['market_cap'] if 'market_cap' in results else None,
    )

async def get_all_ticker_info(tickers, date, num_concurrent=10):
    semaphore = asyncio.Semaphore(num_concurrent)
    async with httpx.AsyncClient() as client:
        tasks = [get_ticker_info(client, semaphore, ticker, date) for ticker in tickers]
        results = await tqdm_asyncio.gather(*tasks, total=len(tasks))
    return results

async def get_close(client, semaphore, ticker, date):
    async with semaphore:
        resp = await client.get(f'https://api.polygon.io/v1/open-close/{ticker}/{date}?adjusted=true&{API_KEY}')
    results = resp.json()
    if 'close' in results:
        close_value = results['close']
    else:
        close_value = None
    return (date, close_value)

async def get_historical_data(ticker, dates, num_concurrent=10):
    semaphore = asyncio.Semaphore(num_concurrent)
    async with httpx.AsyncClient() as client:
        tasks = [get_close(client, semaphore, ticker, date) for date in dates]
        results = await asyncio.gather(*tasks)
    return results

def step_1_get_tickers(date):
    resp = httpx.get(f'https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{date}?adjusted=true&{API_KEY}').json()
    tickers = [r['T'] for r in resp['results']]
    results = asyncio.run(get_all_ticker_info(tickers, date))
    with open(f'results_ticker_info_{date}.pkl', 'wb') as f:
        pickle.dump(results, f)

def step_2_get_top_500():
    with open('results_ticker_info.pkl', 'rb') as f:
        results = pickle.load(f)
    mr = [tinfo for tinfo in results if tinfo.market_cap is not None and tinfo.exchange == 'XNAS' and tinfo.type == 'CS']
    mr = sorted(mr, key=lambda r: r.market_cap, reverse=True)

def step_3_get_historical_data(tickers, start_date, end_date):
    dates = date_range(start_date, end_date)
    results = {}
    for ticker in tqdm(tickers):
        results[ticker] = asyncio.run(get_historical_data(ticker, dates))
    with open('results_n100_historical.pkl', 'wb') as f:
        pickle.dump(results, f)

def compare_performance(start_dt, end_dt):
    from get_mba import MBAResult
    with open('results_n100_historical.pkl', 'rb') as f:
        historical_close: dict[str, tuple[str, float]] = pickle.load(f)
        for ticker in historical_close:
            historical_close[ticker] = dict(historical_close[ticker])
    with open(f'results_ticker_info_{start_dt}.pkl', 'rb') as f:
        start_ticker_info: dict[str, TickerInfo] = pickle.load(f)
        start_ticker_info = {tinfo.ticker: tinfo for tinfo in start_ticker_info if tinfo is not None and tinfo.ticker in n100.N100}
    with open(f'results_ticker_info_{end_dt}.pkl', 'rb') as f:
        end_ticker_info: dict[str, TickerInfo] = pickle.load(f)
        end_ticker_info = {tinfo.ticker: tinfo for tinfo in end_ticker_info if tinfo is not None and tinfo.ticker in n100.N100}
    with open('results_mba_n100.pkl', 'rb') as f:
        mba_results: dict[str, MBAResult] = pickle.load(f)

    # Compare the average stock increase for MBA CEOs and no MBA CEOs.
    to_exclude = set(['ARM', 'GEHC', 'HON']) # Those N100 companies were not listed before. For HON we don't have start market cap.
    @dataclass
    class Info:
        start_close: float
        end_close: float
        start_market_cap: float
        end_market_cap: float
        has_mba: bool
    infos = {}
    for ticker in n100.N100:
        if ticker in to_exclude:
            continue
        if ticker not in start_ticker_info:
            continue
        infos[ticker] = Info(
            historical_close[ticker][start_dt],
            historical_close[ticker][end_dt],
            start_ticker_info[ticker].market_cap,
            end_ticker_info[ticker].market_cap,
            mba_results[ticker].ceo_has_mba
        )
    no_mba = {k: v for k, v in infos.items() if not v.has_mba}
    top_valued = dict(sorted([(k, v) for k,v in infos.items()], key=lambda t: t[1].start_market_cap)[:62])
    # Calculate the number of shares for each company.
    def calc_weights(infos):
        sum_market_cap = sum([i.start_market_cap for i in infos.values()])
        weights = {}
        for ticker, info in infos.items():
            weights[ticker] = info.start_market_cap / sum_market_cap
        return weights
    def calc_returns(infos, weights):
        returns = 0
        for ticker, info in infos.items():
            returns += weights[ticker] * (info.end_close - info.start_close) / info.start_close
        return returns
    no_mba_returns = calc_returns(no_mba, calc_weights(no_mba))
    top_valued_returns = calc_returns(top_valued, calc_weights(top_valued))
    print(f'{start_dt}:{end_dt}', f'no_mba:{round(no_mba_returns, 1)}', f'top_valued:{round(top_valued_returns, 1)}')

if __name__ == '__main__':
    # start_dt, end_dt = '2022-08-29', '2024-08-27'
    # start_dt, end_dt = '2023-08-28', '2024-08-27'
    # step_1_get_tickers(start_dt)
    # step_1_get_tickers(end_dt)
    # step_2_get_top_500()
    # step_3_get_historical_data(n100.N100, '2019-09-03', '2024-08-27')

    start_dt, end_dt = '2019-09-03', '2024-08-27'
    compare_performance(start_dt, end_dt)
    start_dt, end_dt = '2022-08-29', '2024-08-27'
    compare_performance(start_dt, end_dt)
    start_dt, end_dt = '2023-08-28', '2024-08-27'
    compare_performance(start_dt, end_dt)

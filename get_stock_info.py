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

API = f'apiKey={api_config.POLYGON_API_KEY}'
DATE = '2024-08-27'

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

async def get_ticker_info(client, semaphore, ticker):
    async with semaphore:
        resp = await client.get(f'https://api.polygon.io/v3/reference/tickers/{ticker}?{API}')
    results = resp.json()['results']
    return TickerInfo(
        results['ticker'],
        results['primary_exchange'],
        results['cik'] if 'cik' in results else None,
        results['type'],
        results['active'],
        results['market_cap'] if 'market_cap' in results else None,
    )

async def get_all_ticker_info(tickers, num_concurrent=10):
    semaphore = asyncio.Semaphore(num_concurrent)
    async with httpx.AsyncClient() as client:
        tasks = [get_ticker_info(client, semaphore, ticker) for ticker in tickers]
        results = await tqdm_asyncio.gather(*tasks, total=len(tasks))
    return results

async def get_close(client, semaphore, ticker, date):
    async with semaphore:
        resp = await client.get(f'https://api.polygon.io/v1/open-close/{ticker}/{date}?adjusted=true&{API}')
    results = resp.json()
    if 'close' in results:
        close_value = results['close']
    else:
        close_value = None
    return (date, close_value)

async def get_historical_data(ticker, dates, num_concurrent=20):
    semaphore = asyncio.Semaphore(num_concurrent)
    async with httpx.AsyncClient() as client:
        tasks = [get_close(client, semaphore, ticker, date) for date in dates]
        results = await asyncio.gather(*tasks)
    return results

def step_1_get_tickers():
    resp = httpx.get(f'https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{DATE}?adjusted=true&{API}').json()
    tickers = [r['T'] for r in resp['results']]
    results = asyncio.run(get_all_ticker_info(tickers))
    with open('results_ticker_info.pkl', 'wb') as f:
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

if __name__ == '__main__':
    # step_1_get_tickers()
    # step_2_get_top_500()
    # step_3_get_historical_data(n100.N100, '2019-8-30', '2024-08-27')
    # from get_ceo_history import CEOChange
    # with open('results_ceo_changes.pkl', 'rb') as f:
    #     changes = pickle.load(f)
    # changes = {t: c for t, c in changes.items() if t in n100.N100}
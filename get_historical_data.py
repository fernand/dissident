import asyncio
import os
import pickle
from dataclasses import dataclass

import httpx
import tqdm

import api_config
import utils

API_KEY = f'apiKey={api_config.POLYGON_API_KEY}'

@dataclass
class TickerInfo:
    ticker: str
    close: float
    exchange: str
    cik: str | None
    type: str | None
    active: bool
    market_cap: int | None

@dataclass
class NullTickerInfo:
    ticker: str

async def get_ticker_info(client: httpx.AsyncClient, semaphore: asyncio.Semaphore, ticker_close: tuple[str, float], date: str):
    ticker, close = ticker_close
    async with semaphore:
        resp = await client.get(f'https://api.polygon.io/v3/reference/tickers/{ticker}?date={date}&{API_KEY}')
    resp = resp.json()
    if 'results' not in resp:
        return NullTickerInfo(ticker)
    results = resp['results']
    return TickerInfo(
        results['ticker'],
        close,
        results['primary_exchange'],
        results['cik'] if 'cik' in results else None,
        results['type'] if 'type' in results else None,
        results['active'],
        results['market_cap'] if 'market_cap' in results else None,
    )

async def get_all_ticker_info(ticker_closes: list[tuple[str, float]], date: str, num_concurrent=12):
    semaphore = asyncio.Semaphore(num_concurrent)
    async with httpx.AsyncClient() as client:
        tasks = [get_ticker_info(client, semaphore, tc, date) for tc in ticker_closes]
        results = await asyncio.gather(*tasks)
    return results

def get_date(date: str, blacklist: set[str]):
    resp = httpx.get(f'https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{date}?adjusted=true&{API_KEY}').json()
    if resp['resultsCount'] == 0:
        return []
    ticker_close = [(r['T'], r['c']) for r in resp['results'] if r['T'] not in blacklist]
    return asyncio.run(get_all_ticker_info(ticker_close, date))

def get_all_historical_data(start_date: str, end_date: str):
    if os.path.exists('blacklist.pkl'):
        with open('blacklist.pkl', 'rb') as f:
            blacklist: set[str] = pickle.load(f)
    else:
        blacklist: set[str] = set()
    if os.path.exists('historical_data.pkl'):
        with open('historical_data.pkl', 'rb') as f:
            date_results = pickle.load(f)
    else:
        date_results = {}
    for date in tqdm.tqdm(utils.date_range(start_date, end_date)):
        if date in date_results:
            continue
        results = get_date(date, blacklist)
        filtered_results = []
        for info in results:
            match info:
                case NullTickerInfo():
                    blacklist.add(info.ticker)
                case TickerInfo():
                    if info.exchange != 'XNAS' or info.type != 'CS':
                        blacklist.add(info.ticker)
                    else:
                        filtered_results.append(info)
        date_results[date] = filtered_results
        with open('historical_data.pkl', 'wb') as f:
            pickle.dump(date_results, f)
        with open('blacklist.pkl', 'wb') as f:
            pickle.dump(blacklist, f)

if __name__ == '__main__':
    get_all_historical_data('2019-09-11', '2024-09-06')

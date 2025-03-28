import asyncio
import os
import pickle
from datetime import datetime, timedelta

import httpx
import tqdm

import api_config
import utils
from classes import TickerInfo, NullTickerInfo

API_KEY = f'apiKey={api_config.POLYGON_API_KEY}'

@utils.async_retry_with_exponential_backoff
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
        results.get('primary_exchange'),
        results.get('cik'),
        results.get('type'),
        results['active'],
        results.get('market_cap'),
    )

async def get_all_ticker_info(ticker_closes: list[tuple[str, float]], date: str, num_concurrent=8):
    semaphore = asyncio.Semaphore(num_concurrent)
    async with httpx.AsyncClient() as client:
        tasks = [get_ticker_info(client, semaphore, tc, date) for tc in ticker_closes]
        results = await asyncio.gather(*tasks)
    return results

def get_date(date: str, blacklist: set[str]) -> list[TickerInfo | NullTickerInfo]:
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
    dates_to_process = [d for d in utils.date_range(start_date, end_date) if d not in date_results]
    for date in tqdm.tqdm(dates_to_process):
        # Hack if we don't want to wait for the middle data which is usually not needed.
        # if date != end_date:
        #     continue
        results = get_date(date, blacklist)
        filtered_results = []
        for info in results:
            match info:
                case NullTickerInfo():
                    blacklist.add(info.ticker)
                case TickerInfo():
                    if info.exchange not in ['XNAS', 'XNYS'] or info.type != 'CS':
                        blacklist.add(info.ticker)
                    else:
                        filtered_results.append(info)
        date_results[date] = filtered_results
        with open('historical_data.pkl', 'wb') as f:
            pickle.dump(date_results, f)
        with open('blacklist.pkl', 'wb') as f:
            pickle.dump(blacklist, f)

def get_historical_data(dates: list[str]):
    if os.path.exists('dates.pkl'):
        with open('dates.pkl', 'rb') as f:
            date_results = pickle.load(f)
    else:
        date_results = {}
    dates_to_process = [d for d in dates if d not in date_results]
    for date in tqdm.tqdm(dates_to_process):
        results = get_date(date, set())
        filtered_results = []
        for info in results:
            match info:
                case NullTickerInfo():
                    pass
                case TickerInfo():
                    if info.exchange in ['XNAS', 'XNYS'] and info.type == 'CS':
                        filtered_results.append(info)
        date_results[date] = filtered_results
    with open('dates.pkl', 'wb') as f:
        pickle.dump(date_results, f)

def get_top_tickers(date: str, exchanges: list[str] = ['XNYS', 'XNAS'], top_k=1200):
    with open('historical_data.pkl', 'rb') as f:
        data = pickle.load(f)
    blacklist = ['CSGP', 'GOOGL', 'ARCC', 'AJG', 'UHAL', 'BRKR', 'GME']
    filtered = [tinfo for tinfo in data[date] if tinfo.exchange in exchanges]
    return sorted(
        filtered,
        key=lambda info: info.market_cap if info.market_cap is not None and info.ticker not in blacklist else 0,
        reverse=True,
    )[:top_k]

if __name__ == '__main__':
    end_dt = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')
    # get_all_historical_data('2024-08-27', end_dt)
    get_historical_data(['2024-08-27', end_dt])
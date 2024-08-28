import asyncio
import pickle
from dataclasses import dataclass
from typing import Optional

import httpx
from tqdm.asyncio import tqdm_asyncio

import api_config

API = f'apiKey={api_config.POLYGON_API_KEY}'
DATE = '2024-08-27'

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

async def get_all(tickers):
    semaphore = asyncio.Semaphore(10)
    async with httpx.AsyncClient() as client:
        tasks = [get_ticker_info(client, semaphore, ticker) for ticker in tickers]
        results = await tqdm_asyncio.gather(*tasks, total=len(tasks))
    return results

def step_1_get_tickers():
    resp = httpx.get(f'https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{DATE}?adjusted=true&{API}').json()
    tickers = [r['T'] for r in resp['results']]
    results = asyncio.run(get_all(tickers))
    with open('results_ticker_info.pkl', 'wb') as f:
        pickle.dump(results, f)

def step_2_get_top_500():
    with open('results_ticker_info.pkl', 'rb') as f:
        results = pickle.load(f)
    mr = [tinfo for tinfo in results if tinfo.market_cap is not None and tinfo.exchange == 'XNAS' and tinfo.type == 'CS']
    mr = sorted(mr, key=lambda r: r.market_cap, reverse=True)
    print(mr[:10])

if __name__ == '__main__':
    # step_1_get_tickers()
    step_2_get_top_500()

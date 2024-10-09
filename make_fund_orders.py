import asyncio
import json
import traceback

import httpx
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce

import api_config
import utils

PAPI_KEY = f'apiKey={api_config.POLYGON_API_KEY}'

@utils.async_retry_with_exponential_backoff
async def get_close(client: httpx.AsyncClient, semaphore: asyncio.Semaphore, ticker: str, date: str) -> float:
    async with semaphore:
        resp = await client.get(f'https://api.polygon.io/v1/open-close/{ticker}/{date}?adjusted=true&{PAPI_KEY}')
    resp = resp.json()
    return (ticker, resp['close'])

async def get_all_close(tickers: list[str], date: str, num_concurrent=10) -> dict[str, float]:
    semaphore = asyncio.Semaphore(num_concurrent)
    async with httpx.AsyncClient() as client:
        tasks = [get_close(client, semaphore, t, date) for t in tickers]
        results = await asyncio.gather(*tasks)
    return dict(results)

def make_order(trading_client, ticker:str, qty: float):
    if ticker in ['AUR']:
        qty = int(qty)
    market_order_data = MarketOrderRequest(symbol=ticker, qty=qty, side=OrderSide.BUY, time_in_force=TimeInForce.DAY)
    try:
        market_order = trading_client.submit_order(order_data=market_order_data)
    except:
        traceback.print_exc()
        print(ticker, qty)

if __name__ == '__main__':
    trading_client = TradingClient(api_config.ALPACA_PAPER_API_KEY, api_config.ALPACA_PAPER_API_SECRET, paper=True)

    with open('portfolio.json') as f:
        portfolio = json.load(f)

    close = asyncio.run(get_all_close(portfolio.keys(), '2024-10-08'))

    portfolio_amount = 10_000

    for ticker, close in close.items():
        weight = portfolio[ticker]
        qty = portfolio_amount * weight / close
        if qty > 0:
            make_order(trading_client, ticker, qty)

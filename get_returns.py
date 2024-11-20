import json
import pickle

from classes import TickerInfo

def portfolio_returns(portfolio: dict[str, float], start_data: list[TickerInfo], end_data: list[TickerInfo]):
    start_data = dict([(info.ticker, info) for info in start_data])
    end_data = dict([(info.ticker, info) for info in end_data])
    start_value = 0
    end_value = 0
    for ticker, qty in portfolio.items():
        start_value += qty * start_data[ticker].close
        end_value += qty * end_data[ticker].close
    returns = 100 * (end_value - start_value) / start_value
    print(f'Returns: {returns:.1f}%')

if __name__ == '__main__':
    start_dt, end_dt = '2024-10-11', '2024-11-19'
    with open(f'portfolio_{start_dt}.json') as f:
        portfolio = json.load(f)
    with open('historical_data.pkl', 'rb') as f:
        stock_data = pickle.load(f)
    portfolio_returns(portfolio, stock_data[start_dt], stock_data[end_dt])
import json
import pickle

from classes import TickerInfo

def portfolio_returns(portfolio: dict[str, float], start_data: list[TickerInfo], end_data: list[TickerInfo], equal_weighted=False):
    start_data = dict([(info.ticker, info) for info in start_data])
    end_data = dict([(info.ticker, info) for info in end_data])
    returns = 0
    for ticker, weight in portfolio.items():
        stock_return = (end_data[ticker].close - start_data[ticker].close) / start_data[ticker].close
        if equal_weighted:
            returns += 100 * (1 / len(portfolio)) * stock_return
        else:
            returns += 100 * weight * stock_return
    print(f'Returns {'equal weighted' if equal_weighted else ''}: {returns:.1f}%')

if __name__ == '__main__':
    start_dt, end_dt = '2024-08-27', '2024-11-19'
    with open(f'portfolio_2024-10-11.json') as f:
        portfolio = json.load(f)
    with open('historical_data.pkl', 'rb') as f:
        stock_data = pickle.load(f)
    portfolio_returns(portfolio, stock_data[start_dt], stock_data[end_dt], equal_weighted=False)
    portfolio_returns(portfolio, stock_data[start_dt], stock_data[end_dt], equal_weighted=True)
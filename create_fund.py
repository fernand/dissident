import pickle

from historical_data import TickerInfo

if __name__ == '__main__':
    start_dt = '2024-08-27'
    with open('historical_data.pkl', 'rb') as f:
        data = pickle.load(f)
    blacklist = ['CSGP']
    sorted_data = sorted(
        data[start_dt],
        key=lambda info: info.market_cap if info.market_cap is not None and info.ticker not in blacklist else 0,
        reverse=True,
    )
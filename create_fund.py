import pickle

from historical_data import TickerInfo

if __name__ == '__main__':
    # with open('historical_data.pkl', 'rb') as f:
    #     data = pickle.load(f)
    blacklist = ['CSGP']
    sorted_data = sorted(
        data['2019-09-11'],
        key=lambda info: info.market_cap if info.market_cap is not None and info.ticker not in blacklist else 0,
        reverse=True,
    )
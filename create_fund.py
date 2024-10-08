import pickle

import historical_data
from historical_data import TickerInfo
from ceo_info import MBAResult

if __name__ == '__main__':
    start_dt, end_dt = '2024-08-27', '2024-10-07'
    top_tickers = historical_data.get_top_tickers(start_dt, top_k=300)
    with open('results_mba.pkl', 'rb') as f:
        results_mba = pickle.load(f)
    top_no_mba = []
    for tinfo in top_tickers:
        if len(top_no_mba) == 100:
            break
        if tinfo.ticker in results_mba and not results_mba[tinfo.ticker].ceo_has_mba:
            top_no_mba.append(tinfo)
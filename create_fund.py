import json
import pickle

import historical_data
from classes import CEO, TickerInfo

def calc_returns(
    tickers: list[TickerInfo],
    start_data: dict[str, TickerInfo],
    end_data: dict[str, TickerInfo],
    max_weight: float = 0.24  # Maximum allowed weight per company
) -> float:
    total_market_cap = sum(ti.market_cap for ti in tickers)
    initial_weights = {ti.ticker: ti.market_cap / total_market_cap for ti in tickers}

    # Cap weights at max_weight
    capped_weights = {}
    excess_weight = 0.0
    for ticker, weight in initial_weights.items():
        if weight > max_weight:
            capped_weights[ticker] = max_weight
            excess_weight += weight - max_weight
        else:
            capped_weights[ticker] = weight  # Temporarily assign initial weight

    # Identify tickers that are not capped
    non_capped_tickers = [ticker for ticker, weight in initial_weights.items() if weight <= max_weight]

    # Calculate total weight of non-capped tickers
    total_non_capped_weight = sum(initial_weights[ticker] for ticker in non_capped_tickers)

    # Redistribute excess_weight proportionally to non-capped tickers
    for ticker in non_capped_tickers:
        additional_weight = (initial_weights[ticker] / total_non_capped_weight) * excess_weight
        capped_weights[ticker] = initial_weights[ticker] + additional_weight

    total_weight = sum(capped_weights.values())
    assert 0.999 <= total_weight <= 1.001

    portfolio_return = 0.0
    for ti in tickers:
        ticker = ti.ticker
        if ticker not in start_data or ticker not in end_data:
            print(f'{ticker} not found in end_data')
            continue
        start_close = start_data[ticker].close
        end_close = end_data[ticker].close
        portfolio_return += capped_weights[ticker] * (end_close - start_close) / start_close

    return portfolio_return, capped_weights

if __name__ == '__main__':
    prospective = False
    # TODO: Check any changes in CEO between start_dt and end_dt
    fund_size = 20
    start_dt, end_dt = '2024-08-27', '2024-12-05'
    top_tickers = historical_data.get_top_tickers(end_dt if prospective else start_dt)
    with open('dates.pkl', 'rb') as f:
        data = pickle.load(f)
        start_data = {tinfo.ticker: tinfo for tinfo in data[start_dt]}
        end_data = {tinfo.ticker: tinfo for tinfo in data[end_dt]}
    with open(f'results_yahoo_ceo_info_2024-10-07.pkl', 'rb') as f:
        results_ceo: dict[str, CEO] = pickle.load(f)
    with open(f'results_yahoo_ceo_info_2024-08-27.pkl', 'rb') as f:
        results_ceo_old: dict[str, CEO] = pickle.load(f)

    top_founder = []
    for tinfo in top_tickers:
        if len(top_founder) == fund_size:
            break
        if tinfo.ticker in results_ceo and results_ceo[tinfo.ticker].is_founder:
            top_founder.append(tinfo)
    print('Num founder companies', len(top_founder))
    founder_ceo_results, weights = calc_returns(top_founder, start_data, end_data)
    with open('portfolio.json', 'w') as f:
        json.dump(weights, f, indent=4)

    for tinfo in top_founder:
        ticker = tinfo.ticker
        ceo = results_ceo[ticker]
        if ticker in results_ceo_old and ceo.is_founder != results_ceo_old[ticker].is_founder:
            print(f'Founder diff {ticker} old:{ceo.name} new:{results_ceo_old[ticker].name}, new_is_founder:{ceo.is_founder}')

    top_n_tickers = []
    for tinfo in top_tickers:
        if len(top_n_tickers) == len(top_founder):
            break
        top_n_tickers.append(tinfo)
    top_market_cap_results, _ = calc_returns(top_n_tickers, start_data, end_data)

    print(
        f'{start_dt}:{end_dt}',
        f'Founder CEO: {round(founder_ceo_results, 3)}',
        f'Top N market cap: {round(top_market_cap_results, 3)}'
    )

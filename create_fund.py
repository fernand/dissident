import pickle

import historical_data
from historical_data import TickerInfo
from ceo_info import MBAResult, CEOFounderResult

def calc_returns(
    tickers: list[TickerInfo],
    start_data: dict[str, TickerInfo],
    end_data: dict[str, TickerInfo],
    max_weight: float = 0.24  # Maximum allowed weight per company
) -> float:
    total_market_cap = sum(ti.market_cap for ti in tickers)

    initial_weights = {ti.ticker: ti.market_cap / total_market_cap for ti in tickers}

    # Step 1: Cap weights at max_weight
    capped_weights = {}
    excess_weight = 0.0
    for ticker, weight in initial_weights.items():
        if weight > max_weight:
            capped_weights[ticker] = max_weight
            excess_weight += weight - max_weight
        else:
            capped_weights[ticker] = weight  # Temporarily assign initial weight

    # Step 2: Identify tickers that are not capped
    non_capped_tickers = [ticker for ticker, weight in initial_weights.items() if weight <= max_weight]

    # Step 3: Calculate total weight of non-capped tickers
    total_non_capped_weight = sum(initial_weights[ticker] for ticker in non_capped_tickers)

    # Avoid division by zero
    if total_non_capped_weight > 0:
        # Step 4: Redistribute excess_weight proportionally to non-capped tickers
        for ticker in non_capped_tickers:
            additional_weight = (initial_weights[ticker] / total_non_capped_weight) * excess_weight
            capped_weights[ticker] = initial_weights[ticker] + additional_weight
    else:
        # If all tickers are capped, normalize capped weights to sum to 1
        total_capped = sum(capped_weights.values())
        for ticker in capped_weights:
            capped_weights[ticker] /= total_capped

    # Ensure that the total weights sum up to 1 (or very close due to floating point arithmetic)
    total_weight = sum(capped_weights.values())
    if not 0.999 <= total_weight <= 1.001:
        # Normalize weights to sum to 1
        capped_weights = {ticker: weight / total_weight for ticker, weight in capped_weights.items()}

    # Step 5: Calculate portfolio returns using capped weights
    portfolio_return = 0.0
    for ti in tickers:
        ticker = ti.ticker
        if ticker not in start_data or ticker not in end_data:
            print(f'{ticker} not found in end_data')
            continue

        start_close = start_data[ticker].close
        end_close = end_data[ticker].close

        if start_close == 0:
            continue  # Avoid division by zero

        weight = capped_weights.get(ticker, 0.0)
        individual_return = (end_close - start_close) / start_close
        portfolio_return += weight * individual_return

    return portfolio_return

if __name__ == '__main__':
    start_dt, end_dt = '2024-08-27', '2024-10-07'
    top_tickers = historical_data.get_top_tickers(start_dt, top_k=600)
    with open('historical_data.pkl', 'rb') as f:
        data = pickle.load(f)
        start_data = {tinfo.ticker: tinfo for tinfo in data[start_dt]}
        end_data = {tinfo.ticker: tinfo for tinfo in data[end_dt]}
    with open('results_mba.pkl', 'rb') as f:
        results_mba = pickle.load(f)
    with open('results_founder.pkl', 'rb') as f:
        results_founder = pickle.load(f)

    top_no_mba = []
    for tinfo in top_tickers:
        if len(top_no_mba) == 100:
            break
        if tinfo.ticker in results_mba and not results_mba[tinfo.ticker].ceo_has_mba:
            top_no_mba.append(tinfo)
    no_mba_returns = calc_returns(top_no_mba, start_data, end_data)

    top_founder = []
    for tinfo in top_tickers:
        if len(top_founder) == 100:
            break
        if tinfo.ticker in results_founder and results_founder[tinfo.ticker].ceo_is_founder:
            top_founder.append(tinfo)
    print('Num founder companies', len(top_founder))
    founder_ceo_results = calc_returns(top_founder, start_data, end_data)

    print(f'{start_dt}:{end_dt}', f'No MBA :{round(no_mba_returns, 3)}', f'Founder CEO :{round(founder_ceo_results, 3)}')

import pickle

import historical_data
import utils
from classes import TickerInfo, NullTickerInfo, CEO

def step_1_get_yahoo_executives(companies, date):
    from playwright.sync_api import sync_playwright
    @utils.retry_with_exponential_backoff
    @utils.RateLimiter(calls_per_second=0.4)
    def extract_table_html(page, ticker):
        page.goto(f'https://finance.yahoo.com/quote/{ticker}/profile')
        page.wait_for_selector("table", timeout=5000)
        table_html = page.inner_html("table")
        if 'your patience' in table_html:
            raise utils.RateLimitError()
        elif not table_html.startswith('<thead>'):
            print(table_html)
            raise Exception(f'{ticker}: did not get table')
        return table_html
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        def query(company):
            return extract_table_html(page, company['ticker'])
        utils.continue_doing(f'results_yahoo_executives_{date}.pkl', companies, query)

def step2_get_ceo_info(date, ref_companies):
    with open(f'results_yahoo_executives_{date}.pkl', 'rb') as f:
        company_tables = pickle.load(f)
    ref_companies = set([c['ticker'] for c in ref_companies])
    companies = []
    for ticker, table in company_tables.items():
        if ticker in ref_companies:
            companies.append({'ticker': ticker, 'table': table})
    def query(company):
        return utils.get_openai_response(
            "Extract the CEO (also known as Chief Executive Officer) name from the HTML table, and whether the CEO is a Founder.",
            company['table'],
            CEO,
        )
    utils.continue_doing(f'results_yahoo_ceo_info_{date}.pkl', companies, query)

if __name__ == '__main__':
    date = '2024-10-07'
    top_tickers = historical_data.get_top_tickers(date)
    companies = [{'ticker': tinfo.ticker} for tinfo in top_tickers]
    step_1_get_yahoo_executives(companies, date)
    step2_get_ceo_info(date, companies)

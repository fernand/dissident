import json
import pickle
from dataclasses import dataclass
from typing import Optional

from historical_data import TickerInfo, NullTickerInfo
import utils

def step_1_get_yahoo_executives(tickers, date):
    from playwright.sync_api import sync_playwright
    @utils.retry_with_exponential_backoff
    @utils.RateLimiter(calls_per_second=0.4)
    def extract_table_html(page, symbol):
        page.goto(f'https://finance.yahoo.com/quote/{symbol}/profile/')
        page.wait_for_selector("table", timeout=5000)
        table_html = page.inner_html("table")
        if 'your patience' in table_html:
            raise utils.RateLimitError()
        elif not table_html.startswith('<thead>'):
            print(table_html)
            raise Exception(f'{symbol}: did not get table')
        return table_html
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        def query(symbol):
            return extract_table_html(page, symbol)
        utils.continue_doing(f'results_yahoo_executives_{date}.pkl', tickers, query)

def step_2_create_yahoo_ceo_batch(date):
    batch = []
    with open(f'results_yahoo_executives_{date}.pkl', 'rb') as f:
        company_tables = pickle.load(f)
    for symbol, table in company_tables.items():
        batch.append({
            'custom_id': symbol,
            'method': 'POST',
            'url': '/v1/chat/completions',
            'body': {
                'model': 'gpt-4o-mini',
                'messages': utils.openai_chat_template(
                    'Extract the CEO (also known as Chief Executive Officer) name and year born from the HTML table.',
                    table
                ),
                'max_tokens': 1000,
                'response_format': {
                    'type': 'json_schema',
                    'json_schema': {
                        'name': 'ceo',
                        'schema': {
                            'type': 'object',
                            "properties": {
                                'ceo_name': {'type': 'string'},
                                'year_born': {'type': 'string'},
                            },
                            'required': ['ceo_name', 'year_born'],
                            'additionalProperties': False,
                        },
                        'strict': True,
                    },
                },
            }
        })
    with open(f'batches/get_yahoo_ceo_batch_{date}.jsonl', 'w') as f:
        f.write('\n'.join([json.dumps(item) for item in batch]))

@dataclass
class CurrentCEO:
    name: str
    year_born: Optional[str]

def step_3_compile_yahoo_current_ceos(date):
    current_ceos = {}
    with open('batches/batch_RylVMJyaTzkxcmhhZ6Mle9Y3_output.jsonl') as f:
        for l in f:
            result = json.loads(l.rstrip())
            ticker = result['custom_id']
            data = json.loads(result['response']['body']['choices'][0]['message']['content'])
            ceo_name, year_born = data['ceo_name'], data['year_born']
            if len(ceo_name) == 0 or ceo_name is None:
                print(ticker, ceo_name, year_born)
                continue
            if len(year_born) == 0:
                year_born = None
            current_ceos[ticker] = CurrentCEO(ceo_name, year_born)
    with open(f'results_yahoo_current_ceos_{date}.pkl', 'wb') as f:
        pickle.dump(current_ceos, f)

if __name__ == '__main__':
    date = '2024-08-27'
    with open('historical_data.pkl', 'rb') as f:
        h = pickle.load(f)
    tickers = [tinfo.ticker for tinfo in h[date]]
    step_1_get_yahoo_executives(tickers, date)
    step_2_create_yahoo_ceo_batch(date)
    step_3_compile_yahoo_current_ceos(date)

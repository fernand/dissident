import json
import pickle
from dataclasses import dataclass

import lxml.html
import httpx

import utils

def format_cik(cik):
    return '0' * (10 - len(cik)) + cik

@dataclass
class Form:
    url: str
    date: str
    has_502: bool

@utils.RateLimiter(calls_per_second=10)
def get_8k_metadata(cik, cumul_urls):
    url = f'https://efts.sec.gov/LATEST/search-index?from={len(cumul_urls)}&dateRange=custom&ciks={cik}&startdt=2001-01-01&forms=8-K'
    response = httpx.get(url, headers={'User-Agent': 'Chrome/128.0.0.0'})
    if response.status_code == 200:
        data = json.loads(response.text)
        total_hits = data['hits']['total']['value']
        current_hits = data['hits']['hits']
        for hit in current_hits:
            f1, f2 = hit['_id'].split(':')
            f1 = f1.replace('-', '')
            url = '/'.join((f"https://www.sec.gov/Archives/edgar/data/{cik.lstrip('0')}", f1, f2))
            has_502 = ('items' in hit and '5.02' in hit['items']) or ('_source' in hit and '5.02' in hit['_source']['items'])
            if 'file_date' in hit:
                file_date = hit['file_date']
            else:
                file_date = hit['_source']['file_date']
            cumul_urls.append(Form(url, file_date, has_502))
        if total_hits - len(cumul_urls) > 0:
            return get_8k_metadata(cik, cumul_urls)
        else:
            return cumul_urls
    else:
        print(f'Error: Unable to fetch data. {response.status_code} {response.text}')
        return []

def get_text(node):
    text = ''
    if node.text is not None:
        text += node.text
    text += '\n'.join([get_text(child) for child in node])
    if node.tail:
        text += node.tail
    return text

@utils.RateLimiter(calls_per_second=10)
def get_8k(url):
    response = httpx.get(url, headers={
        'User-Agent': 'Your Name (your.email@example.com)',
        'Accept-Encoding': 'gzip, deflate, br',
    })
    tree = lxml.html.fromstring(response.content)
    return get_text(tree)

def step_1_get_8k_metadata(companies):
    def query(company):
        return get_8k_metadata(format_cik(str(company['cik'])), cumul_urls=[])
    utils.continue_doing('results_8kforms.pkl', companies, query)

def step_2_get_8k_forms(companies):
    with open('results_8kforms.pkl', 'rb') as f:
        forms = pickle.load(f)
    for company in companies:
        company['forms'] = forms[company['symbol']]
    def query(company):
        new_forms = []
        for form in company['forms']:
            if form.has_502:
                new_forms.append({'form': form, 'text': get_8k(form.url)})
        return new_forms
    utils.continue_doing('results_8kform_text.pkl', companies, query)

EXTRACT_SECTION_PROMPT = """In the 8-K form, extract the Item 5.02 section only."""

def step_3_count_form_tokens():
    import tiktoken
    encoding = tiktoken.encoding_for_model('gpt-4o-mini')
    system_prompt_len = len(encoding.encode(EXTRACT_SECTION_PROMPT))
    with open('results_8kform_text.pkl', 'rb') as f:
        forms = pickle.load(f)
    count = 0
    for company_forms in forms.values():
        for form in company_forms:
            count += system_prompt_len + len(encoding.encode(form['text']))
    print('num M tokens', round(count / 1e6, 1))
    print(f'GPT-4o-Mini batched cost: ${round(0.15 / 2 * count / 1e6, 1)}')

def step_4_create_section_batch(companies):
    with open('results_8kform_text.pkl', 'rb') as f:
        forms = pickle.load(f)
    for company in companies:
        company['forms'] = forms[company['symbol']]
    batch = []
    for company in companies:
        last_date = None
        i = 0
        for form_dict in company['forms']:
            form, text = form_dict['form'], form_dict['text']
            if 'CEO' not in text and 'Chief Executive Officer' not in text:
                continue
            if form.date == last_date:
                i += 1
            else:
                i = 0
            last_date = form.date
            batch.append({
                'custom_id': f"{company['symbol']}_{form.date}_{i}",
                'method': 'POST',
                'url': '/v1/chat/completions',
                'body': {
                    'model': 'gpt-4o-mini',
                    'messages': [
                        {
                            'role': 'system',
                            'content': EXTRACT_SECTION_PROMPT,
                        },
                        {
                            'role': 'user',
                            'content': text,
                        }
                    ],
                    'max_tokens': 1000,
                }
            })
    for i, chunk in enumerate(utils.chunks(batch, 5)):
        with open(f'get_section_batch{i}.jsonl', 'w') as f:
            f.write('\n'.join([json.dumps(item) for item in chunk]))

CEO_CHANGE_PROMPT = """Identify whether there is a change of Chief Executive Officer. If there is no change, return empty values. If there is a change, find the name of the new CEO / Chief Executive Officer, and the name of the departing CEO. We are NOT interested in other offices than CEO, so ignore any other names linked to a role other than Chief Executive Officer."""
RESULT_BATCH_FILES = [
    'batch_9spadxw8qCibONWCC6VHpl5H_output.jsonl',
    'batch_d3Z8e29cQ0pNX8scp9q5eM54_output.jsonl',
    'batch_gEDtZXFWhsE4fQ7WJn5lY3PP_output.jsonl',
    'batch_IynbsAdjLiDgF09mdu4QazHg_output.jsonl',
    'batch_V15ZiTiMLAkTdoy41lnnlPUr_output.jsonl',
]

def step_5_count_section_tokens():
    import tiktoken
    encoding = tiktoken.encoding_for_model('gpt-4o-2024-08-06')
    system_prompt_len = len(encoding.encode(CEO_CHANGE_PROMPT))
    count = 0
    for batch_file in RESULT_BATCH_FILES:
        with open(batch_file) as f:
            for line in f:
                section = json.loads(line.rstrip())['response']['body']['choices'][0]['message']['content']
                if 'CEO' not in section and 'Chief Executive Officer' not in section:
                    continue
                count += system_prompt_len + len(encoding.encode(section))
    print('num M tokens', round(count / 1e6, 1))
    print(f'GPT-4o batched cost: ${round(1.25 * count / 1e6, 1)}')

def step_6_create_ceo_change_batch():
    batch = []
    for batch_file in RESULT_BATCH_FILES:
        with open(batch_file) as f:
            for line in f:
                result = json.loads(line.rstrip())
                section = result['response']['body']['choices'][0]['message']['content']
                if 'CEO' not in section and 'Chief Executive Officer' not in section:
                    continue
                batch.append({
                    'custom_id': result['custom_id'],
                    'method': 'POST',
                    'url': '/v1/chat/completions',
                    'body': {
                        'model': 'gpt-4o-2024-08-06',
                        'messages': utils.openai_chat_template(CEO_CHANGE_PROMPT, section),
                        'max_tokens': 1000,
                        'response_format': {
                            'type': 'json_schema',
                            'json_schema': {
                                'name': 'ceo_change',
                                'schema': {
                                    'type': 'object',
                                    "properties": {
                                        'previous_ceo_name': {'type': 'string'},
                                        'new_ceo_name': {'type': 'string'},
                                    },
                                    'required': ['previous_ceo_name', 'new_ceo_name'],
                                    'additionalProperties': False,
                                },
                                'strict': True,
                            },
                        },
                    }
                })
    with open(f'get_ceo_change_batch.jsonl', 'w') as f:
        f.write('\n'.join([json.dumps(item) for item in batch]))

def step_7_get_yahoo_executives(companies):
    from playwright.sync_api import sync_playwright
    def extract_table_html(symbol):
        page = browser.new_page()
        page.goto(f'https://finance.yahoo.com/quote/{symbol}/profile/')
        page.wait_for_selector("table")
        table_html = page.inner_html("table")
        if not table_html.startswith('<tbody>'):
            print(table_html)
            raise Exception(f'{symbol}: did not get table')
        return table_html
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        @utils.RateLimiter(calls_per_second=0.25)
        def query(company):
            return extract_table_html(company['symbol'])
        utils.continue_doing('results_yahoo_executives.pkl', companies, query)

if __name__ == '__main__':
    companies = utils.get_nasdaq_companies()
    # step_1_get_8k_metadata(companies)
    # step_2_get_8k_forms(companies)
    # step_3_count_form_tokens()
    # step_4_create_section_batch(companies)
    # step_5_count_section_tokens()
    # step_6_create_ceo_change_batch()
    # Gather all the step_4 batch result errors and manually look for CEO changes.
    step_7_get_yahoo_executives(companies)

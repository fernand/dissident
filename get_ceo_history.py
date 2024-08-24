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
def get_8k_forms(cik, cumul_urls):
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
            return get_8k_forms(cik, cumul_urls)
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

def step_1(companies):
    def query(company):
        return get_8k_forms(format_cik(str(company['cik'])), cumul_urls=[])
    utils.continue_doing('results_8kforms.pkl', companies, query)

def step_2(companies):
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
CEO_CHANGE_PROMPT = """Identify whether there is a change of Chief Executive Officer. If there is no change, return empty values. If there is a change, find the name of the new CEO / Chief Executive Officer, and the name of the departing CEO. We are NOT interested in other offices than CEO, so ignore any other names linked to a role other than Chief Executive Officer."""

def step_3_count_tokens():
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
    print(f'GPT-4o-Mini cost: ${round(0.15 * count / 1e6, 1)}')

def create_section_batch(companies):
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
                'custom_id': f'{company['symbol']}_{form.date}_{i}',
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
    def chunks(l, n):
        for i in range(0, n):
            yield l[i::n]
    for i, chunk in enumerate(chunks(batch, 5)):
        with open(f'get_section_batch{i}.jsonl', 'w') as f:
            f.write('\n'.join([json.dumps(item) for item in chunk]))

def create_ceo_change_batch():
    pass

if __name__ == '__main__':
    companies = utils.get_nasdaq_companies()
    # step_1(companies)
    # step_2(companies)
    # step_3_count_tokens()
    create_section_batch(companies)

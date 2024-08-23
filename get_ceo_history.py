import json
import pickle
from dataclasses import dataclass
from typing import Optional

import lxml.html
import httpx
from pydantic import BaseModel

import utils

CEO_CHANGE_PROMPT = """Find the name of the company which is filing the 8-K document. For that company, look for any change of CEO / Chief Executive Officer (and **ONLY** this office) in the Item 5.02 section. Then find (if any) the departing and new CEO names."""

def format_cik(cik):
    return '0' * (10 - len(cik)) + cik

@dataclass
class Form:
    url: str
    date: str
    has_502: bool

@utils.RateLimiter(calls_per_second=10)
def get_8k_forms(cik, cumul_urls=[]):
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
        print(f"Error: Unable to fetch data. {response.status_code} {response.text}")
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
        return get_8k_forms(format_cik(str(company['cik'])))
    results_path = 'results_8kforms.pkl'
    utils.continue_doing(results_path, companies, query)

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
    results_path = 'results_8kform_text.pkl'
    utils.continue_doing(results_path, companies, query)

def step_3_count_tokens():
    import tiktoken
    encoding = tiktoken.encoding_for_model('gpt-4o-mini')
    system_prompt_len = len(encoding.encode(CEO_CHANGE_PROMPT))
    with open('results_8kform_text.pkl', 'rb') as f:
        forms = pickle.load(f)
    count = 0
    for company_forms in forms.values():
        for form in company_forms:
            count += system_prompt_len + len(encoding.encode(form['text']))
    print('num M tokens', round(count / 1e6, 1))
    print(f'GPT-4o-Mini cost: ${0.15 * count / 1e6}')

class CEOChange(BaseModel):
    company_name: str
    previous_ceo_name: Optional[str]
    new_ceo_name: Optional[str]

def get_ceo_change(text):
    return utils.get_openai_response(
        utils.openai_client,
        CEO_CHANGE_PROMPT,
        text,
        CEOChange,
    )

@dataclass
class CEOChangeWithDate:
    date: str
    previous_ceo_name: Optional[str]
    new_ceo_name: Optional[str]

def step_4(companies):
    with open('results_8kform_text.pkl', 'rb') as f:
        forms = pickle.load(f)
    filtered_companies = []
    for company in companies:
        # TODO: Change
        if company['symbol'] in forms:
            company['forms'] = forms[company['symbol']]
            filtered_companies.append(company)
    companies = [c for c in filtered_companies if c['symbol'] == 'MSFT']
    def query(company):
        ceo_changes = []
        for form_dict in company['forms']:
            form, text = form_dict['form'], form_dict['text']
            ceo_change = get_ceo_change(text)
            prev_ceo = ceo_change.previous_ceo_name if ceo_change.previous_ceo_name != 'null' else None
            new_ceo = ceo_change.new_ceo_name if ceo_change.new_ceo_name != 'null' else None
            if prev_ceo is None and new_ceo is None:
                continue
            ceo_changes.append(CEOChangeWithDate(
                date=form.date,
                previous_ceo_name=prev_ceo,
                new_ceo_name=new_ceo,
            ))
        return ceo_changes
    results_path = 'results_ceo_changes.pkl'
    utils.continue_doing(results_path, companies, query)

if __name__ == '__main__':
    companies = utils.get_nasdaq_companies()
    # step_1(companies)
    step_2(companies)
    # step_3_count_tokens()
    # step_4(companies)

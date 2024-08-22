import json
import os
import pickle
from dataclasses import dataclass
from typing import Optional

import lxml.html
import httpx
from pydantic import BaseModel

import utils

def format_cik(cik):
    return '0' * (10 - len(cik)) + cik

@dataclass
class Form:
    url: str
    date: str
    has_502: bool

@utils.rate_limiter(calls_per_second=10)
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
            url = '/'.join(('https://www.sec.gov/Archives/edgar/data/1633917', f1, f2))
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

@utils.rate_limiter(calls_per_second=10)
def get_8k(url):
    response = httpx.get(url, headers={'User-Agent': 'Your Name (your.email@example.com)'})
    tree = lxml.html.fromstring(response.content)
    return get_text(tree)

class CEOChange(BaseModel):
    company_name: str
    previous_ceo_name: Optional[str]
    new_ceo_name: Optional[str]

def get_ceo_change(text):
    return utils.get_openai_response(
        utils.openai_client,
        """Find the name of the company which is filing the 8-K document. For that company, look for any change of CEO / Chief Executive Offcie (and **ONLY** this office) in the Item 5.02 section. Then find (if any) the departing and new CEO names.""",
        text,
        CEOChange,
    )

@dataclass
class CEOChangeWithDate:
    date: str
    previous_ceo_name: Optional[str]
    new_ceo_name: Optional[str]

def process_forms(forms: list[Form]):
    ceo_changes = []
    for form in forms:
        if not form.has_502:
            continue
        ceo_change = get_ceo_change(get_8k(form.url))
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

if __name__ == '__main__':
    # data = get_8k_urls(format_cik('1633917'))
    # data = get_8k_urls(format_cik('68505'))
    # url = 'https://www.sec.gov/Archives/edgar/data/1633917/000119312523212353/d475247d8k.htm'
    # url = 'https://www.sec.gov/Archives/edgar/data/1633917/000163391716000222/a8-kready.htm'
    # ceo_change = get_ceo_change(get_8k(url))

    results_path = 'results_8kforms.pkl'
    companies = utils.get_nasdaq_companies()
    def query(company):
        return get_8k_forms(company['cik'])
    utils.continue_doing(results_path, companies, query)

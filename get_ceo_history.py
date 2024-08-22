import json

import httpx

import utils

@utils.rate_limiter(calls_per_second=10)
def get_8k_urls(cik, cumul_urls=[]):
    url = f'https://efts.sec.gov/LATEST/search-index?from={len(cumul_urls)}&dateRange=custom&ciks={cik}&startdt=2001-01-01&forms=8-K'
    response = httpx.get(url, headers={'User-Agent': 'Chrome/128.0.0.0'})
    if response.status_code == 200:
        data = json.loads(response.text)
        total_hits = data['hits']['total']['value']
        current_hits = data['hits']['hits']
        for hit in current_hits:
            f1, f2 = hit['_id'].split(':')
            f1 = f1.replace('-', '')
            cumul_urls.append('/'.join(('https://www.sec.gov/Archives/edgar/data/1633917', f1, f2)))
        if total_hits - len(cumul_urls) > 0:
            return get_8k_urls(cik, cumul_urls)
        else:
            return cumul_urls
    else:
        print(f"Error: Unable to fetch data. {response.status_code} {response.text}")
        return []

data = get_8k_urls('0001633917')
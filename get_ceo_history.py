import json

import lxml.html
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
            url = '/'.join(('https://www.sec.gov/Archives/edgar/data/1633917', f1, f2))
            has_502 = '5.02' in hit['items']
            cumul_urls.append((url, has_502))
        if total_hits - len(cumul_urls) > 0:
            return get_8k_urls(cik, cumul_urls)
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
    url = 'https://www.sec.gov/Archives/edgar/data/1633917/000163391716000222/a8-kready.htm'
    response = httpx.get(url, headers={'User-Agent': 'Your Name (your.email@example.com)'})
    html_content = response.text.replace('&nbsp;', ' ')
    tree = lxml.html.fromstring(html_content)
    return get_text(tree)

if __name__ == '__main__':
    # data = get_8k_urls('0001633917')
    pass

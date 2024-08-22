import json

import httpx

def get_502_forms(cik):
    url = f"https://efts.sec.gov/LATEST/search-index?dateRange=custom&ciks={cik}&startdt=2010-01-01&forms=8-K"
    response = httpx.get(url, headers={'User-Agent': 'Chrome/128.0.0.0'})
    if response.status_code == 200:
        return json.loads(response.text)
    else:
        print(f"Error: Unable to fetch data. Status code: {response.status_code}")
        return None

data = get_502_forms('0001633917')
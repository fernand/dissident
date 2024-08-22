import requests
import json

def get_cik_from_ticker(ticker):
    url = "https://www.sec.gov/files/company_tickers.json"
    response = requests.get(url, headers={'User-Agent': 'Company Name CompanyEmail@domain.com'})
    if response.status_code == 200:
        data = json.loads(response.text)

        for _, company_info in data.items():
            if company_info['ticker'] == ticker.upper():
                # CIK found, return it (zero-padded to 10 digits)
                return str(company_info['cik_str']).zfill(10)

        return None
    else:
        print(f"Error: Unable to fetch data. Status code: {response.status_code}")
        return None

ticker = "PYPL"
cik = get_cik_from_ticker(ticker)

if cik:
    print(f"The CIK for {ticker} is: {cik}")
else:
    print(f"No CIK found for ticker {ticker}")
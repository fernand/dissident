import pickle
from dataclasses import dataclass

from pydantic import BaseModel

import historical_data
import utils
from get_ceo import CurrentCEO

class CompanyName(BaseModel):
    name: str

class CEOName(BaseModel):
    name: str

class CEOHasMBA(BaseModel):
    has_mba: bool

@dataclass
class MBAResult:
    ticker:str|None = None
    ceo_name:str|None = None
    ceo_has_mba:bool|None = None
    ceo_mba_response:str|None = None

def ceo_mba_question(ceo_name, company_name):
    return f"Does {ceo_name.lstrip('Mr. ').lstrip('Ms. ').lstrip('Dr. ')}, CEO of {company_name} have an MBA or MBA like degree?"

def mba_query(company):
    ceo_mba_response = utils.get_perplexity_response(ceo_mba_question(company['ceo_name'], company['ticker']))
    print(ceo_mba_response)
    ceo_has_mba = utils.get_openai_response(
        "Extract the true/false value whether the CEO has an MBA or MB like degree.",
        ceo_mba_response,
        CEOHasMBA,
    ).has_mba
    print(ceo_has_mba)
    print()
    return MBAResult(
        company['ticker'],
        company['ceo_name'],
        ceo_has_mba,
        ceo_mba_response,
    )

if __name__ == '__main__':
    with open('results_yahoo_current_ceos.pkl', 'rb') as f:
        current_ceos = pickle.load(f)

    date = '2024-08-27'
    top_tickers = historical_data.get_top_tickers(date, 300)
    top_tickers = set([tinfo.ticker for tinfo in top_tickers])

    companies = []
    for ticker, current_ceo in current_ceos.items():
        if ticker in top_tickers:
            companies.append({'ticker': ticker, 'ceo_name': current_ceo.name})

    utils.continue_doing('results_mba.pkl', companies, mba_query)

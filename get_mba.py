from dataclasses import dataclass

from pydantic import BaseModel

import n100
import utils

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
    return f"Does {ceo_name}, CEO of {company_name} have an MBA or MBA like degree?"

def mba_query(company):
    ceo_mba_response = utils.get_perplexity_response(ceo_mba_question(company['ceo_name'], company['symbol']))
    print(ceo_mba_response)
    ceo_has_mba = utils.get_openai_response(
        "Extract the true/false value whether the CEO has an MBA or MB like degree.",
        ceo_mba_response,
        CEOHasMBA,
    ).has_mba
    print(ceo_has_mba)
    print()
    return MBAResult(
        company['symbol'],
        company['ceo_name'],
        ceo_has_mba,
        ceo_mba_response,
    )

if __name__ == '__main__':
    results_path = 'results_mba_n100.pkl'
    companies = [{'symbol': c[0], 'ceo_name': c[1]} for c in n100.CEOS]
    def query(company):
        return mba_query(company)
    utils.continue_doing('results_mba_n100.pkl', companies, mba_query)

from dataclasses import dataclass

from pydantic import BaseModel

import utils

class CompanyName(BaseModel):
    name: str

class CEOName(BaseModel):
    name: str

class CEOHasMBA(BaseModel):
    has_mba: bool

@dataclass
class MBAResult:
    symbol:str = None
    company_name:str = None
    ceo_name:str = None
    ceo_has_mba:bool = None
    ceo_response:str = None
    ceo_mba_response:str = None

def ceo_question(company_name, symbol):
    return f"Who is the CEO of {company_name} (stock ticker {symbol})"

def ceo_mba_question(ceo_name, company_name):
    return f"Does {ceo_name}, CEO of {company_name} have an MBA or MBA like degree?"

def mba_query(openai_client, perplexity_client, company_name, symbol):
    ceo_response = utils.get_perplexity_response(perplexity_client, ceo_question(company_name, symbol))
    print(ceo_response)
    ceo_name = utils.get_openai_response(
        openai_client,
        "Extract the name of the CEO.",
        ceo_response,
        CEOName,
    ).name
    print(ceo_name)
    ceo_mba_response = utils.get_perplexity_response(perplexity_client, ceo_mba_question(ceo_name, company_name))
    print(ceo_mba_response)
    ceo_has_mba = utils.get_openai_response(
        openai_client,
        "Extract the true/false value whether the CEO has an MBA or MB like degree.",
        ceo_mba_response,
        CEOHasMBA,
    ).has_mba
    print(ceo_has_mba)
    print()
    return MBAResult(
        symbol,
        company_name,
        ceo_name,
        ceo_has_mba,
        ceo_response,
        ceo_mba_response,
    )

if __name__ == '__main__':
    results_path = 'results_mba.pkl'
    companies = utils.get_nasdaq_companies()

    def query(company):
        return mba_query(utils.openai_client, utils.perplexity_client, company['name'], company['symbol'])
    utils.continue_doing(mba_query)

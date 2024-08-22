from dataclasses import dataclass
import json
import os
import pickle
import random
import time

import openai
from pydantic import BaseModel

OPENAI_API_KEY = "sk-svcacct-Rm8mE1eL2npMQ90EYva-orGqH5LAIJ2zTKrgIG1HvWOZ05SJivT3BlbkFJKfXMhhcq72cc3p9jkourbUTO4cCSagzuAKzrGaKDKp7ToPxYMA"
PERPLEXITY_API_KEY = "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"

def retry_with_exponential_backoff(
    func,
    initial_delay: float = 1,
    exponential_base: float = 2,
    jitter: bool = True,
    max_retries: int = 10,
    errors: tuple = (openai.RateLimitError,),
):
    """Retry a function with exponential backoff."""

    def wrapper(*args, **kwargs):
        # Initialize variables
        num_retries = 0
        delay = initial_delay

        # Loop until a successful response or max_retries is hit or an exception is raised
        while True:
            try:
                return func(*args, **kwargs)

            # Retry on specific errors
            except errors as e:
                # Increment retries
                num_retries += 1

                # Check if max retries has been reached
                if num_retries > max_retries:
                    raise Exception(f"Maximum number of retries ({max_retries}) exceeded.")

                # Increment the delay
                delay *= exponential_base * (1 + jitter * random.random())

                # Sleep for the delay
                time.sleep(delay)

            # Raise exceptions for any errors not specified
            except Exception as e:
                raise e

    return wrapper

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

def openai_chat_template(instruction, query_result):
    return [
        {
            "role": "system",
            "content": instruction,
        },
        {
            "role": "user",
            "content": query_result,
        },
    ]

def perplexity_chat_template(message):
    return [
        {
            "role": "system",
            "content": "Be concise.",
        },
        {
            "role": "user",
            "content": message,
        },
    ]

@retry_with_exponential_backoff
def get_openai_response(client, instruction, query_result, format):
    completion = client.beta.chat.completions.parse(
        model='gpt-4o-mini',
        messages=openai_chat_template(instruction, query_result),
        response_format=format,
    )
    return completion.choices[0].message.parsed

@retry_with_exponential_backoff
def get_perplexity_response(client, message):
    response = client.chat.completions.create(
        model="llama-3.1-sonar-small-128k-online",
        messages=perplexity_chat_template(message),
    )
    return response.choices[0].message.content

def mba_query(openai_client, perplexity_client, company_name, symbol):
    ceo_response = get_perplexity_response(perplexity_client, ceo_question(company_name, symbol))
    print(ceo_response)
    ceo_name = get_openai_response(
        openai_client,
        "Extract the name of the CEO.",
        ceo_response,
        CEOName,
    ).name
    print(ceo_name)
    ceo_mba_response = get_perplexity_response(perplexity_client, ceo_mba_question(ceo_name, company_name))
    print(ceo_mba_response)
    ceo_has_mba = get_openai_response(
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
    companies = []
    with open('company_tickers_exchange.json') as f:
        for row in json.load(f)['data']:
            cik, company_name, symbol, exchange = row
            if exchange == 'Nasdaq':
                companies.append({'name': company_name, 'symbol': symbol})

    openai_client = openai.OpenAI(api_key=OPENAI_API_KEY)
    perplexity_client = openai.OpenAI(api_key=PERPLEXITY_API_KEY, base_url="https://api.perplexity.ai")

    if os.path.exists(results_path):
        with open(results_path, 'rb') as f:
            results = pickle.load(f)
    else:
        results = {}

    for company in companies[10:]:
        symbol = company['symbol']
        if symbol in results:
            continue
        try:
            results[symbol] = mba_query(openai_client, perplexity_client, company['name'], symbol)
        except Exception as e:
            print(e)
            continue
        with open(results_path, 'wb') as f:
            pickle.dump(results, f)

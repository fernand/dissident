import json
import os
import pickle
import random
import time
import traceback
from functools import wraps

import openai
from tqdm import tqdm

OPENAI_API_KEY = 'sk-svcacct-Rm8mE1eL2npMQ90EYva-orGqH5LAIJ2zTKrgIG1HvWOZ05SJivT3BlbkFJKfXMhhcq72cc3p9jkourbUTO4cCSagzuAKzrGaKDKp7ToPxYMA'
PERPLEXITY_API_KEY = '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'

openai_client = openai.OpenAI(api_key=OPENAI_API_KEY)
perplexity_client = openai.OpenAI(api_key=PERPLEXITY_API_KEY, base_url='https://api.perplexity.ai')

class RateLimiter:
    def __init__(self, calls_per_second):
        self.min_interval = 1.0 / calls_per_second
        self.last_call_time = 0.0

    def __call__(self, func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            current_time = time.perf_counter()
            elapsed = current_time - self.last_call_time
            if elapsed < self.min_interval:
                sleep_time = self.min_interval - elapsed
                time.sleep(sleep_time)
            self.last_call_time = time.perf_counter()
            return func(*args, **kwargs)

        return wrapper

def retry_with_exponential_backoff(
    func,
    initial_delay: float = 1,
    exponential_base: float = 2,
    jitter: bool = True,
    max_retries: int = 10,
    errors: tuple = (openai.RateLimitError,),
):
    """Retry a function with exponential backoff."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        num_retries = 0
        delay = initial_delay
        while True:
            try:
                return func(*args, **kwargs)
            except errors as e:
                num_retries += 1
                if num_retries > max_retries:
                    raise Exception(f"Maximum number of retries ({max_retries}) exceeded.")
                delay *= exponential_base * (1 + jitter * random.random())
                time.sleep(delay)
            except Exception as e:
                raise e

    return wrapper

def openai_chat_template(instruction, query_result):
    template = [
        {
            'role': 'system',
            'content': instruction,
        },
    ]
    if len(query_result) == 0:
        return template
    template.append({
            'role': 'user',
            'content': query_result,
    })
    return template

def perplexity_chat_template(message):
    return [
        {
            'role': 'system',
            'content': 'Be concise.',
        },
        {
            'role': 'user',
            'content': message,
        },
    ]

@retry_with_exponential_backoff
def get_openai_response(client, instruction, query_result, format, model='gpt-4o-mini'):
    completion = client.beta.chat.completions.parse(
        model=model,
        messages=openai_chat_template(instruction, query_result),
        response_format=format,
    )
    return completion.choices[0].message.parsed

@retry_with_exponential_backoff
def get_perplexity_response(client, message):
    response = client.chat.completions.create(
        model='llama-3.1-sonar-small-128k-online',
        messages=perplexity_chat_template(message),
    )
    return response.choices[0].message.content

# From https://www.sec.gov/files/company_tickers_exchange.json
def get_nasdaq_companies():
    companies = []
    with open('company_tickers_exchange.json') as f:
        for row in json.load(f)['data']:
            cik, company_name, symbol, exchange = row
            if exchange == 'Nasdaq':
                companies.append({'cik': cik, 'name': company_name, 'symbol': symbol})
    return companies

def continue_doing(results_path, companies, func, save_every=5):
    if os.path.exists(results_path):
        with open(results_path, 'rb') as f:
            results = pickle.load(f)
    else:
        results = {}

    count = 0
    for company in tqdm(companies):
        symbol = company['symbol']
        if symbol in results:
            continue
        try:
            results[symbol] = func(company)
        except Exception as e:
            traceback.print_exc()
            continue
        count += 1
        if count == save_every:
            with open(results_path, 'wb') as f:
                pickle.dump(results, f)
            count = 0
    with open(results_path, 'wb') as f:
        pickle.dump(results, f)

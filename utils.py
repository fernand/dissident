import asyncio
import os
import pickle
import random
import time
import traceback
from datetime import datetime, timedelta
from functools import wraps

import openai
from tqdm import tqdm

from api_config import *

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

class RateLimitError(Exception):
    pass

def retry_with_exponential_backoff(
    func,
    initial_delay: float = 1,
    exponential_base: float = 2,
    jitter: bool = True,
    max_retries: int = 10,
    errors: tuple = (openai.RateLimitError, RateLimitError),
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

def async_retry_with_exponential_backoff(
    func,
    initial_delay: float = 1,
    exponential_base: float = 2,
    jitter: bool = True,
    max_retries: int = 10,
    errors: tuple = (Exception, ),
):
    """Retry an async function with exponential backoff."""
    @wraps(func)
    async def wrapper(*args, **kwargs):
        num_retries = 0
        delay = initial_delay
        while True:
            try:
                return await func(*args, **kwargs)
            except errors as e:
                traceback.print_exc()
                num_retries += 1
                if num_retries > max_retries:
                    raise Exception(f"Maximum number of retries ({max_retries}) exceeded.")
                delay *= exponential_base * (1 + jitter * random.random())
                await asyncio.sleep(delay)
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
def get_openai_response(instruction, query_result, format, model='gpt-4o-mini'):
    completion = openai_client.beta.chat.completions.parse(
        model=model,
        messages=openai_chat_template(instruction, query_result), # type: ignore
        response_format=format,
    )
    return completion.choices[0].message.parsed

@retry_with_exponential_backoff
def get_perplexity_response(message):
    response = perplexity_client.chat.completions.create(
        model='llama-3.1-sonar-small-128k-online',
        messages=perplexity_chat_template(message),
    )
    return response.choices[0].message.content

def continue_doing(results_path, companies, func, save_every=5):
    if os.path.exists(results_path):
        with open(results_path, 'rb') as f:
            results = pickle.load(f)
    else:
        results = {}

    count = 0
    for company in tqdm(companies):
        ticker = company['ticker']
        if ticker in results:
            continue
        try:
            results[ticker] = func(company)
        except Exception as e:
            traceback.print_exc()
            print(ticker, '\n')
            continue
        count += 1
        if count == save_every:
            with open(results_path, 'wb') as f:
                pickle.dump(results, f)
            count = 0
    with open(results_path, 'wb') as f:
        pickle.dump(results, f)

def date_range(start_date, end_date):
    start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
    end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
    dates = []
    current_date = start_date
    while current_date <= end_date:
        dates.append(current_date.strftime('%Y-%m-%d'))
        current_date += timedelta(days=1)
    return dates

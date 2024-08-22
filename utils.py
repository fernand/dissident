import random
import time
from functools import wraps

import openai

OPENAI_API_KEY = "sk-svcacct-Rm8mE1eL2npMQ90EYva-orGqH5LAIJ2zTKrgIG1HvWOZ05SJivT3BlbkFJKfXMhhcq72cc3p9jkourbUTO4cCSagzuAKzrGaKDKp7ToPxYMA"
PERPLEXITY_API_KEY = "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"

openai_client = openai.OpenAI(api_key=OPENAI_API_KEY)
perplexity_client = openai.OpenAI(api_key=PERPLEXITY_API_KEY, base_url="https://api.perplexity.ai")

def rate_limiter(calls_per_second):
    """Rate limit decorator to restrict the function call rate to `calls_per_second`."""

    min_interval = 1.0 / calls_per_second

    def decorator(func):
        last_call_time = [0.0]

        @wraps(func)
        def wrapper(*args, **kwargs):
            current_time = time.perf_counter()
            elapsed = current_time - last_call_time[0]
            if elapsed < min_interval:
                sleep_time = min_interval - elapsed
                print(f"Rate limit exceeded. Waiting for {sleep_time:.2f} seconds.")
                time.sleep(sleep_time)

            last_call_time[0] = time.perf_counter()
            return func(*args, **kwargs)

        return wrapper

    return decorator

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

        # Loop until a successful response or max_retries is hit or an exception is raised
        while True:
            try:
                return func(*args, **kwargs)

            # Retry on specific errors
            except errors as e:
                num_retries += 1
                if num_retries > max_retries:
                    raise Exception(f"Maximum number of retries ({max_retries}) exceeded.")
                delay *= exponential_base * (1 + jitter * random.random())
                time.sleep(delay)

            # Raise exceptions for any errors not specified
            except Exception as e:
                raise e

    return wrapper

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

import random
import time
from functools import wraps

import openai

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

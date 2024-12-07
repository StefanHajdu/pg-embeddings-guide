import time
from functools import wraps

from constants import TIME_LOG_PATH


def measure(func):
    @wraps(func)
    def measure_wrapper(*args, **kwargs):
        start = time.perf_counter()
        result = func(*args, **kwargs)
        end = time.perf_counter()
        print(
            f"{func.__name__} {kwargs.get("chunk_size", "")} -> {1000*(end - start)} ms"
        )
        with open(TIME_LOG_PATH, "a") as f:
            f.write(
                f"{func.__name__} {kwargs.get("chunk_size", "")} -> {1000*(end - start)} ms\n"
            )
        return result

    return measure_wrapper


def async_measure(func):
    @wraps(func)
    async def measure_wrapper(*args, **kwargs):
        start = time.perf_counter()
        result = await func(*args, **kwargs)
        end = time.perf_counter()
        print(
            f"{func.__name__} {kwargs.get("chunk_size", "")} -> {1000*(end - start)} ms"
        )
        with open(TIME_LOG_PATH, "a") as f:
            f.write(
                f"{func.__name__} {kwargs.get("chunk_size", "")} -> {1000*(end - start)} ms\n"
            )
        return result

    return measure_wrapper

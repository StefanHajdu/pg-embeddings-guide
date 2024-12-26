from functools import wraps
import time


def async_measure(coro):
    @wraps(coro)
    async def measure_wrapper(*args, **kwargs):
        start = time.perf_counter()
        result = await coro(*args, **kwargs)
        end = time.perf_counter()
        print(f"{coro.__name__}: {(end - start)} s")
        return result

    return measure_wrapper


def measure(func):
    @wraps(func)
    def measure_wrapper(*args, **kwargs):
        start = time.perf_counter()
        result = func(*args, **kwargs)
        end = time.perf_counter()
        print(f"{func.__name__}: {(end - start)} s")
        return result

    return measure_wrapper

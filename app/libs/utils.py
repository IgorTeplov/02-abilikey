import asyncio
import time
from functools import wraps
from libs.logs import logger, error_logger
from traceback import format_exc


def deduplicate(array):
    new_array = []
    used_ids = []
    for item in array:
        if item["id"] not in used_ids:
            used_ids.append(item["id"])
            new_array.append(item)
    return new_array


def chunks(array, length):
    for i in range(0, len(array), length):
        yield array[i:i + length]


def timeit(func):
    async def wrapper(*args, **kwargs):
        start = time.monotonic()
        answer = await func(*args, **kwargs)
        logger.info(f"Duration: {time.monotonic() - start}")
        return answer
    return wrapper


def rate_limit(seconds: float, tag: str = 'default', lock=None, _request_counter=None):
    def decorator(func):
        nonlocal lock
        last_call_time = {}
        if tag not in last_call_time:
            last_call_time[tag] = 0
        if lock is None:
            lock = asyncio.Lock()

        @wraps(func)
        async def wrapper(*args, **kwargs):
            nonlocal last_call_time
            async with lock:
                now = time.monotonic()
                elapsed = now - last_call_time[tag]
                if elapsed < seconds:
                    await asyncio.sleep(seconds - elapsed)
                if _request_counter:
                    while _request_counter["started"] - _request_counter["ended"] > _request_counter["max_active_requests"]:
                        await asyncio.sleep(.3)
                    if _request_counter["stop"]:
                        logger.warn("Warning! We have detected an anomaly in the duration of requests! Stop the entire system for 35 seconds to cool down the rate limits!")
                        await asyncio.sleep(35)
                        _request_counter["stop"] = False
                last_call_time[tag] = time.monotonic()
            return await func(*args, **kwargs)

        return wrapper

    return decorator


def add_error_handler(error_handler=lambda args, traceback, error: print("Error")):
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            answer = None
            try:
                answer = await func(*args, **kwargs)
            except Exception as e:
                try:
                    error_handler(args=(args, kwargs), traceback=format_exc(), error=e)
                except Exception:
                    error_logger.error("Error in error handler")
                error_logger.error(format_exc())
            return answer
        return wrapper
    return decorator


def repeat_when_429_or_5xx(func):
    @rate_limit(0.2, "repeats", asyncio.Lock())
    async def wrapper(*args, **kwargs):
        answer = await func(*args, **kwargs)
        counter = 0
        while (answer[1] == 429 or answer[1] >= 500) and counter < 5:
            print(f"Requests recive {answer[1]} code.")
            print("Sleep...")
            await asyncio.sleep(31)
            print("Continue...")
            answer = await func(*args, **kwargs)
            counter += 1
        return answer
    return wrapper

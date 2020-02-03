import logging
import time
from functools import wraps

logging.basicConfig(level=logging.INFO)


def timefunc(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        logging.info('execution time: {} seconds'.format(time.time() - start))
        return result
    return wrapper

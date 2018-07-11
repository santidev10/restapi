from itertools import groupby, count


def chunks_generator(iterable, size=10):
    c = count()
    for _, g in groupby(iterable, lambda _: next(c)//size):
        yield g


def safe_exception(logger):
    def decorator(func):
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as err:
                logger.exception(err)
        return wrapper
    return decorator

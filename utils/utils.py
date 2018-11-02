from collections import Counter
from itertools import groupby, count

from utils.lang import flatten


def chunks_generator(iterable, size=10):
    c = count()
    for _, g in groupby(iterable, lambda _: next(c) // size):
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


def _get_all_constant_values(cls):
    values = [value for key, value in cls.__dict__.items() if not key.startswith("_")]

    values_unnamed = [_get_all_constant_values(value) if isinstance(value, type) else [value]
                      for value in values]
    return [value for value in flatten(values_unnamed) if isinstance(value, str)]


def unique_constant_tree(cls):
    values = _get_all_constant_values(cls)
    duplicates = [item for item, value_count in Counter(values).items() if value_count > 1]
    if len(duplicates) > 0:
        raise RuntimeError("{} class has duplicated constants: {}".format(str(cls), ", ".join(duplicates)))
    return cls


def get_all_class_constants(cls):
    return sorted([
        value
        for name, value in cls.__dict__.items()
        if not name.startswith("_")
    ])

import hashlib
import string
from collections import Counter
from django.db.models.query import QuerySet
from itertools import count
from itertools import groupby

from es_components.iab_categories import HIDDEN_IAB_CATEGORIES
from utils.lang import flatten


def chunks_generator(iterable, size=10):
    c = count()
    for _, g in groupby(iterable, lambda _: next(c) // size):
        yield g


def chunked_queryset(queryset: QuerySet, chunk_size=100):
    """
    Slice a queryset into chunks
    :param queryset:
    :param chunk_size:
    :return:
    """
    start = 0

    while True:
        end = start + chunk_size
        chunk = queryset[start:end]
        yield chunk
        if len(chunk) < chunk_size:
            break

        start += chunk_size

def safe_exception(logger):
    def decorator(func):
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except BaseException as err:
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


def convert_subscriber_count(s):
    if s is None:
        return None
    try:
        subscribers = int(s)
    except BaseException:
        try:
            units = s[-1].lower()
            s = s[:-1]
            subs_float = float(s)
            if units == "k":
                subscribers = int(subs_float * 1000)
            elif units == "m":
                subscribers = int(subs_float * 1000000)
            elif units == "b":
                subscribers = int(subs_float * 1000000000)
        except BaseException:
            return 0
    return subscribers


def prune_iab_categories(iab_categories):
    return [category for category in iab_categories if category not in HIDDEN_IAB_CATEGORIES]


def remove_tags_punctuation(s):
    """
    This function replaces any Punctuation Character (except @$#*) from the passed string with a white space:
    Punctuation Characters: !\"%&\'()+,-./:;<=>?[\\]^_`{|}~
    """
    result = ''
    if isinstance(s, str) and len(s) > 0:
        result = s
        # Get the punctuation characters list except: @$#*
        punctuation_str = string.punctuation.replace("@", "").replace("$", "").replace("#", "").replace("*", "")
        if len(punctuation_str) > 0:
            # Create a string of white spaces with the same exact length as punctuation characters string
            white_spaces_str = ' ' * len(punctuation_str)
            # Replace any punctuation character (except @$#*) with a white space
            result = s.translate(str.maketrans(punctuation_str, white_spaces_str)).strip()
    return result


def slice_generator(data_generator, limit):
    counter = 0
    for item in data_generator:
        if counter >= limit:
            break

        yield item
        counter += 1

def get_hash(s):
    """
    :s: str
    :return: int, hashed string
    """
    return int(hashlib.sha256(s.encode("utf-8")).hexdigest(), 16) % 10 ** 8

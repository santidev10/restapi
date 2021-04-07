import hashlib
import string
from collections import Counter
from django.db.models.query import QuerySet
from itertools import count
from itertools import groupby
from typing import Union

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


def validate_youtube_url(url, url_type) -> str:
    """
    Return string youtube id if url is valid
    Otherwise return None if unable to extract id
    :param url: str
    :param url_type: int
    :return:
    """
    if url_type in {0, "video"}:
        splits = ["?v=", "/video/"]
        target = 11
    else:
        splits = ["/channel/"]
        target = 24
    for split in splits:
        val = url.split(split)[-1]
        if len(val) == target:
            return val


def get_hash(s):
    """
    :s: str
    :return: int, hashed string
    """
    return int(hashlib.sha256(s.encode("utf-8")).hexdigest(), 16) % 10 ** 8


class RunningAverage:

    def __init__(self):
        self.count = 0
        self.average = 0.0

    def update(self, value: Union[float, int]) -> Union[float, str]:
        """
        increment the average, returns new value
        :param value:
        :return:
        """
        self.count += 1
        self.average += (value - self.average) / self.count
        return self.get()

    def get(self, pretty=True) -> Union[float, str]:
        """
        get the current running average
        :return:
        """
        average = self.average
        if pretty:
            average = f"{round(average, 2):,}"
        return average

    def get_count(self) -> int:
        """
        return the number of iterations or samples
        :return:
        """
        return self.count

    @staticmethod
    def running_average(count: int, value: float, average: float):
        """
        get a running average given count, value, average
        :param count:
        :param value:
        :param average:
        :return:
        """
        average += (value - average) / count
        return average

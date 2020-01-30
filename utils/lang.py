import re
from collections import namedtuple
from enum import Enum
from functools import reduce
from types import GeneratorType
from typing import Sequence

from fasttext.FastText import _FastText as FastText

fast_text_model = None


def flatten(l):
    return [item for sublist in l for item in sublist]


def safe_index(l, item, default=None):
    try:
        return l.index(item)
    except ValueError:
        return default


def pick_dict(item: dict, keys: Sequence[str]) -> dict:
    return {key: value
            for key, value in item.items()
            if key in keys}


class ExtendedEnum(Enum):
    @classmethod
    def values(cls):
        return (item.value for item in cls)

    @classmethod
    def names(cls):
        return (item.name for item in cls)

    @classmethod
    def has_value(cls, value):
        return value in cls.values()

    @classmethod
    def map_object(cls):
        map_cls = namedtuple("{}Map".format(cls.__name__), cls.names())
        return map_cls(**{key: value.value for key, value in cls.__members__.items()})

    @classmethod
    def choices(cls):
        return tuple((item.name, item.value) for item in cls)


def merge_dicts(*dicts):
    return reduce(lambda res, item: {**res, **item}, dicts, {})


def get_request_prefix(request):
    return "https://" if request.is_secure() else "http://"


def convert_sequence_items_to_sting(sequence):
    """
    Convert all items in sequence to string
    :param sequence: sequence
    :return: set of str items
    """
    return {str(item) for item in sequence}


def almost_equal(value_1, value_2, delta=1e-6):
    return abs(value_1 - value_2) < delta


# Remove @mentions from string
def remove_mentions(s):
    mentions_regex = re.compile("(@[^\s]+)")
    return mentions_regex.sub("", s)


# Remove #hashtags from string
def remove_hashtags(s):
    hashtag_regex = re.compile("(@#[^\s]+)")
    return hashtag_regex.sub("", s)


# Remove http links from string
def remove_links(s):
    links_regex = re.compile("(http[^\s]+)")
    return links_regex.sub("", s)


# Cleans string by removing mentions, hashtags, and links
def remove_mentions_hashes_urls(s):
    return remove_links(remove_hashtags(remove_mentions(s)))


def replace_apostrophes(s):
    apostrophe_regex = re.compile("(&#39;)")
    return apostrophe_regex.sub("'", s)


# Returns Language Detected by FastText
def fasttext_lang(s):
    global fast_text_model
    s = remove_mentions_hashes_urls(s)
    s = s.replace("\n", " ")
    if fast_text_model is None:
        fast_text_model = FastText('lid.176.bin')
    fast_text_result = fast_text_model.predict(s)
    language = fast_text_result[0][0].split('__')[2].lower()
    return language


def is_english(s):
    try:
        s.encode(encoding='utf-8').decode('ascii')
        return True
    except UnicodeDecodeError:
        return False


def merge_sort(generators, key=None):
    """
    Implementation of the merge sort algorithm. It assumes that all incoming generators/iterators are presorted
    """
    key = key or (lambda x: x)
    wrappers = [_GeneratorWrapper(g) for g in generators]
    while True:
        wrappers = [w for w in wrappers if w.has_next]
        if len(wrappers) == 0:
            return
        next_wrapper = min(wrappers, key=lambda w: key(w.head))
        yield next_wrapper.head
        next_wrapper.shift()


class _GeneratorWrapper:
    def __init__(self, generator):
        self.generator = (generator
                          if isinstance(generator, GeneratorType)
                          else as_generator(generator))
        self._head = None
        self._has_next = None
        self.shift()

    @property
    def has_next(self):
        return self._has_next

    @property
    def head(self):
        return self._head

    def shift(self):
        try:
            self._head = next(self.generator)
        except StopIteration:
            self._has_next = False
        else:
            self._has_next = True


def as_generator(items):
    yield from items

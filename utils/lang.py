from enum import Enum
from functools import reduce
from typing import Sequence


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
    def has_value(cls, value):
        return value in cls.values()


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

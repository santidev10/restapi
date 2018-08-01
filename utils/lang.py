from functools import reduce
from typing import List


def flatten(l):
    return [item for sublist in l for item in sublist]


def safe_index(l, item, default=None):
    try:
        return l.index(item)
    except ValueError:
        return default


def pick_dict(item: dict, keys: List[str]):
    return {key: value
            for key, value in item.items()
            if key in keys}


def get_all_class_constants(cls):
    return sorted([value for name, value in cls.__dict__.items()
                   if not name.startswith("_")])


def deep_getattr(obj, attr, default=None):
    try:
        return reduce(getattr, attr.split("."), obj)
    except AttributeError:
        return default

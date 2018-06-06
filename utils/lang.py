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
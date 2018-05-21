from functools import reduce


def flatten(l):
    return [item for sublist in l for item in sublist]


def safe_index(l, item, default=None):
    try:
        return l.index(item)
    except ValueError:
        return default


def _append_item(acc, key_fn, item):
    key = key_fn(item)
    acc[key] = acc.get(key, []) + [item]
    return acc


def groupby_dict(items, key):
    key_fn = lambda i: i.get(key) if isinstance(key, str) else key
    return reduce(lambda res, item: _append_item(res, key_fn, item),
                  items, dict())

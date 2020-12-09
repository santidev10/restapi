from utils.es_components_cache import get_from_cache
from utils.es_components_cache import set_to_cache


def cache_method(timeout):
    def wrapper(method):
        def wrapped(obj, *args, **kwargs):
            options = (args, kwargs)
            part = method.__name__
            try:
                data, _ = get_from_cache(obj, part=part, options=options)
            except BaseException:
                data = None
            if data is None:
                data = method(obj, *args, **kwargs)
                set_to_cache(obj, part=part, options=options, data=data, timeout=timeout)
            return data
        return wrapped
    return wrapper

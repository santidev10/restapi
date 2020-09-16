import logging
import pickle

from django.conf import settings

from utils.redis import get_redis_client

DEFAULT_PAGE_SIZE = 50

logger = logging.getLogger(__name__)
redis = get_redis_client()

CACHE_KEY_PREFIX = "restapi.ESQueryset"


def cached_method(timeout):
    def wrapper(method):
        def wrapped(obj, *args, **kwargs):
            options = (args, kwargs)
            part = method.__name__

            try:
                from_cache = obj.from_cache if obj.from_cache is not None else settings.ES_CACHE_ENABLED
            except AttributeError:
                from_cache = settings.ES_CACHE_ENABLED

            data = get_from_cache(obj, part=part, options=options) if from_cache else None
            if data is None:
                data = method(obj, *args, **kwargs)
                set_to_cache(obj, part=part, options=options, data=data, timeout=timeout)
            return data

        return wrapped

    return wrapper


def flush_cache():
    if settings.ES_CACHE_ENABLED:
        # pylint: disable=redefined-outer-name
        redis = get_redis_client()
        # pylint: enable=redefined-outer-name
        keys = redis.keys(f"{CACHE_KEY_PREFIX}.*")
        if keys:
            redis.delete(*keys)


def get_from_cache(obj, part, options):
    key, key_json = obj.get_cache_key(part, options)
    cached = redis.get(key)
    if cached:
        cached = pickle.loads(cached)
        cached = cached.get("data") if cached and key_json == cached.get("key_json") else None
    return cached


def set_to_cache(obj, part, options, data, timeout):
    key, key_json = obj.get_cache_key(part, options)
    serialized_data = pickle.dumps(dict(key_json=key_json, data=data))
    redis.set(key, serialized_data, timeout)

import logging
import pickle

from utils.redis import get_redis_client

logger = logging.getLogger(__name__)

CACHE_TIMEOUT = 86400

def get_percentiles(manager, fields, add_suffix=None):
    redis = get_redis_client()
    percentiles = {}
    for field in fields:
        key = get_redis_key(model=manager.model, field=field)
        value = redis.get(key)
        if not value:
            continue
        name = key if add_suffix is None else f"{field}{add_suffix}"
        percentiles[name] = pickle.loads(value)
    return percentiles


def update_percentiles(manager):
    redis = get_redis_client()
    for field in manager.percentiles_aggregation_fields:
        logger.info("Get percentiles for %s.%s", manager.model.__name__, field)
        values = manager.fetch_percentiles(field=field)
        key = get_redis_key(model=manager.model, field=field)
        redis.set(key, pickle.dumps(values), CACHE_TIMEOUT)
        logger.info(values)


def get_redis_key(model, field):
    key = f"aggregation_percentiles:{model.__name__}.{field}"
    return key

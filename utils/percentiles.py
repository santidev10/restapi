from collections import OrderedDict
import logging
import pickle
from statistics import mean

from utils.redis import get_redis_client

logger = logging.getLogger(__name__)

CACHE_TIMEOUT = 86400

def get_percentiles(manager, fields, add_suffix=None):
    redis = get_redis_client()
    percentiles = {}
    for field in fields:
        key = get_redis_key(model=manager.model, field=field)
        value = pickle.loads(redis.get(key))
        name = key if add_suffix is None else f"{field}{add_suffix}"
        percentiles[name] = value
    return percentiles


def update_percentiles(manager):
    redis = get_redis_client()
    for field in manager.percentiles_aggregation_fields:
        logger.info("Get percentiles for %s.%s", manager.model.__name__, field)
        values = fetch_percentiles(model=manager.model, field=field)
        key = get_redis_key(model=manager.model, field=field)
        redis.set(key, pickle.dumps(values), CACHE_TIMEOUT)
        logger.info(values)


def get_redis_key(model, field):
    key = f"aggregation_percentiles:{model.__name__}.{field}"
    return key

def fetch_percentiles(model, field):
    settings = model._index.get_settings()
    number_of_shards = None
    for index_name, index_settings in settings.items():
        number_of_shards = int(index_settings["settings"]["index"]["number_of_shards"])
        break

    aggregations = {
        "aggs": {
            "percentiles": {
                "field": field,
                "percents": [10, 20, 30, 40, 50, 60, 70, 80, 90]
            }
        }
    }

    sharded_percentiles = []
    for shard in range(number_of_shards):
        result = model.search()\
            .params(preference=f"_shards:{shard}")\
            .query()\
            .update_from_dict({"aggs": aggregations, "size": 0})\
            .execute().aggregations.aggs["values"].to_dict()
        sharded_percentiles.append(result)

    def aggregate(func, shards, key):
        value = func([shard[key] for shard in shards])
        return value

    percentiles = OrderedDict()
    percentiles["10.0"] = aggregate(min, sharded_percentiles, "10.0")
    for key in ["20.0", "30.0", "40.0", "50.0", "60.0", "70.0", "80.0"]:
        percentiles[key] = aggregate(mean, sharded_percentiles, key)
    percentiles["90.0"] = aggregate(max, sharded_percentiles, "90.0")
    return percentiles
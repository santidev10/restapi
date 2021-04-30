from collections import namedtuple
import pickle

from redis.exceptions import ConnectionError

from cache.constants import RESEARCH_CHANNELS_DEFAULT_CACHE_KEY
from cache.constants import RESEARCH_VIDEOS_DEFAULT_CACHE_KEY
from channel.constants import RESEARCH_CHANNELS_DEFAULT_SORT
from es_components.managers import ChannelManager
from es_components.managers import VideoManager
from utils.redis import get_redis_client
from video.constants import RESEARCH_VIDEOS_DEFAULT_SORT

MAX_SIZE = 50
CACHE_TTL = 14400


CacheConfig = namedtuple("CacheConfig", ("manager", "cache_key", "sort"))


def cache_research_defaults():
    """
    Cache Research first pages
    :return:
    """
    try:
        redis = get_redis_client()
    except ConnectionError:
        return

    configs = (
        CacheConfig(ChannelManager, RESEARCH_CHANNELS_DEFAULT_CACHE_KEY, RESEARCH_CHANNELS_DEFAULT_SORT),
        CacheConfig(VideoManager, RESEARCH_VIDEOS_DEFAULT_CACHE_KEY, RESEARCH_VIDEOS_DEFAULT_SORT)
    )
    for config in configs:
        _cache(redis, config)


def _cache(redis, config):
    manager = config.manager(sections=config.manager.allowed_sections, upsert_sections=())
    data = manager.search(manager.forced_filters(), limit=MAX_SIZE).sort(*config.sort).execute()
    redis.set(config.cache_key, pickle.dumps(data), ex=CACHE_TTL)

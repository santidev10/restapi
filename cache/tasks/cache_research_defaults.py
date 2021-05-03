from collections import namedtuple
import pickle

from django.conf import settings
from redis.exceptions import ConnectionError

from cache.constants import RESEARCH_CHANNELS_DEFAULT_CACHE_KEY
from cache.constants import RESEARCH_VIDEOS_DEFAULT_CACHE_KEY
from channel.constants import RESEARCH_CHANNELS_DEFAULT_SORT
from es_components.managers import ChannelManager
from es_components.managers import VideoManager
from saas import celery_app
from utils.redis import get_redis_client
from utils.celery.tasks import celery_lock
from video.constants import RESEARCH_VIDEOS_DEFAULT_SORT

CACHE_TTL = 14400
CACHE_LOCK_KEY = "cache_research_defaults"


CacheConfig = namedtuple("CacheConfig", ("manager", "cache_key", "sort"))


@celery_app.task(bind=True)
@celery_lock(CACHE_LOCK_KEY, expire=60 * 10, max_retries=0)
def cache_research_defaults_task():
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
    data = manager.search(manager.forced_filters(), limit=settings.CACHE_RESEARCH_DEFAULT_MAX_PAGE_SIZE)\
        .sort(*config.sort).execute()
    redis.set(config.cache_key, pickle.dumps(data), ex=CACHE_TTL)

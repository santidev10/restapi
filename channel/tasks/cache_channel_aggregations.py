from saas import celery_app
from cache.models import CacheItem

@celery_app.task()
def cache_channel_aggregations():
    pass

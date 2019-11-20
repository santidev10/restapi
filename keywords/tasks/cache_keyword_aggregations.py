from saas import celery_app
from cache.models import CacheItem

@celery_app.task()
def cache_keyword_aggregations():
    pass

from saas import celery_app
from cache.models import CacheItem

@celery_app.task()
def gather_video_aggregations():
    pass

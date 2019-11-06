from saas import celery_app
# from utils.models import CacheItem

@celery_app.task()
def gather_channel_aggregations():
    pass

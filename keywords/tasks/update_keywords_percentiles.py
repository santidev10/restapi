from saas import celery_app
from es_components.managers import KeywordManager
from utils.percentiles import update_percentiles


@celery_app.task()
def update_keywords_percentiles():
    update_percentiles(KeywordManager)

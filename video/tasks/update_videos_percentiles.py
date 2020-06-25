from es_components.managers import VideoManager
from saas import celery_app
from utils.percentiles import update_percentiles


@celery_app.task()
def update_videos_percentiles():
    update_percentiles(VideoManager)

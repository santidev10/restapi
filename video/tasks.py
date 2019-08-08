from saas import celery_app
from es_components.managers import VideoManager
from utils.percentiles import update_percentiles


@celery_app.task()
def update_videos_percentiles():
    update_percentiles(VideoManager)
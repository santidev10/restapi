from es_components.managers import ChannelManager
from saas import celery_app
from utils.percentiles import update_percentiles


@celery_app.task()
def update_channels_percentiles():
    update_percentiles(ChannelManager)

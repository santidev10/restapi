from brand_safety.tasks.channel_update import channel_update
from brand_safety.tasks.constants import Schedulers
from es_components.constants import MAIN_ID_FIELD
from es_components.constants import Sections
from es_components.managers import ChannelManager
from es_components.query_builder import QueryBuilder
from saas.configs.celery import Queue
from saas import celery_app
from utils.celery.tasks import celery_lock
from utils.celery.utils import get_queue_size
from utils.utils import chunks_generator


@celery_app.task
@celery_lock(Schedulers.ChannelDiscovery.NAME)
def channel_discovery_scheduler():
    channel_manager = ChannelManager()
    query = channel_manager.forced_filters() \
            & QueryBuilder().build().must_not().exists().field(Sections.BRAND_SAFETY).get()
    # queue_size = get_queue_size(Queue.BRAND_SAFETY_CHANNEL_PRIORITY)
    queue_size = get_queue_size(Queue.HOURLY_STATISTIC)
    limit = Schedulers.ChannelDiscovery.MAX_QUEUE_SIZE - queue_size
    if limit > 0:
        channels = channel_manager.search(query, limit=limit, sort=("-stats.subscribers",)).execute()
        channel_ids = [item.main.id for item in channels]
        for batch in chunks_generator(channel_ids, size=Schedulers.ChannelDiscovery.TASK_BATCH_SIZE):
            # channel_update.delay(list(batch))
            channel_update(list(batch))

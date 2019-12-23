import os

from celery import chain
from celery import group

from brand_safety.constants import CHANNEL_DISCOVERY_LOCK
from brand_safety.constants import CHANNEL_UPDATE_LOCK
from brand_safety.constants import VIDEO_UPDATE_LOCK

from brand_safety.constants import CHANNEL_DISCOVERY_TRACKER

from utils.celery.tasks import REDIS_CLIENT
from utils.celery.tasks import group_chorded
from utils.celery.tasks import unlock
from brand_safety.tasks.audit_manager import AuditManager
from  brand_safety.tasks.channel_discovery import channel_discovery


def main():
    concurrency = os.environ.get("aws_env", "") ==
    channel_discovery_acquired = REDIS_CLIENT.lock(CHANNEL_DISCOVERY_LOCK, expire=60 * 60).acquire(blocking=False)
    if channel_discovery_acquired:
        manager = AuditManager(1)
        cursor = manager.get_cursor(CHANNEL_DISCOVERY_TRACKER)
        to_update = manager.next_batch(cursor.cursor_id)
        try:
            last_id = to_update[-1].id
        except IndexError:
            last_id = None

        group = [channel_discovery.si(batch) for batch in to_update]

        chain(
            group.apply_async(),
            finalize.si(cursor, to_update)
        )

        # get channels to update
        # group tasks
        #  call back should update cursor?

def chain_group():
    pass


def finalize(cursor, cursor_id):
    cursor.cursor_id = cursor_id
    cursor.save()
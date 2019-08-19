import logging
from saas import celery_app
from django.core import management

logger = logging.getLogger(__name__)


@celery_app.task
def segmented_audit_next_channels_batch():
    from audit_tool.segmented_audit import SegmentedAudit
    audit = SegmentedAudit()
    logger.info("Started segmented audit for the next batch of channels")
    channels_count, videos_count = audit.run()
    logger.info("Done (channels_count={}, videos_count={})".format(channels_count, videos_count))


@celery_app.task
def audit_cache_meta():
    management.call_command("audit_cache_meta")


@celery_app.task
def audit_channel_meta():
    management.call_command("audit_channel_meta", thread_id=0)


@celery_app.task
def audit_export():
    management.call_command("audit_export")


@celery_app.task
def audit_video_meta():
    management.call_command("audit_video_meta", thread_id=0)


@celery_app.task
def audit_fill_channel_data():
    management.call_command("audit_fill_channel_data", thread_id=0)


@celery_app.task
def audit_recommended_engine():
    management.call_command("audit_recommended_engine", thread_id=0)


@celery_app.task
def audit_recommended_engine_2():
    management.call_command("audit_recommended_engine", thread_id=1)

import logging

from saas import celery_app

logger = logging.getLogger(__name__)


@celery_app.task
def segmented_audit_next_channels_batch():
    # pylint: disable=import-outside-toplevel
    from audit_tool.segmented_audit import SegmentedAudit
    # pylint: enable=import-outside-toplevel
    audit = SegmentedAudit()
    logger.info("Started segmented audit for the next batch of channels")
    channels_count, videos_count = audit.run()
    logger.info("Done (channels_count=%s, videos_count=%s)", channels_count, videos_count)

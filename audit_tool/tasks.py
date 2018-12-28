import logging
from saas import celery_app

logger = logging.getLogger(__name__)


@celery_app.task
def segmented_audit_next_channels_batch():
    from audit_tool.segmented_audit import SegmentedAudit
    audit = SegmentedAudit()
    logger.info("Started segmented audit for the next batch of channels")
    channels_count, videos_count = audit.run()
    logger.info("Done (channels_count={}, videos_count={})".format(channels_count, videos_count))


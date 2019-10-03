import logging
from saas import celery_app
from django.core.management import call_command

logger = logging.getLogger(__name__)


@celery_app.task
def segmented_audit_next_channels_batch():
    from audit_tool.segmented_audit import SegmentedAudit
    audit = SegmentedAudit()
    logger.info("Started segmented audit for the next batch of channels")
    channels_count, videos_count = audit.run()
    logger.info("Done (channels_count={}, videos_count={})".format(channels_count, videos_count))

@celery_app.task
def pull_custom_transcripts():
    logger.info("Pulling custom transcripts.")
    call_command("pull_custom_transcripts")
    logger.info("Finished pulling 10,000 custom transcripts.")

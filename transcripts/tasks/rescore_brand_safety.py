import logging
from saas import celery_app
from brand_safety.auditors.brand_safety_audit import BrandSafetyAudit

logger = logging.getLogger(__name__)

@celery_app.task
def rescore_brand_safety_videos(vid_ids=None):
    logger.info("Rescoring brand safety for videos with new Watson transcripts.")
    auditor = BrandSafetyAudit()
    auditor.process_videos(vid_ids or [])
    logger.info("Finished rescoring brand safety for videos with new Watson transcripts.")

import logging

from brand_safety.auditors.brand_safety_audit import BrandSafetyAudit
from saas import celery_app
from saas.configs.celery import TaskExpiration
from utils.celery.tasks import lock
from utils.celery.tasks import unlock

logger = logging.getLogger(__name__)

RESCORE_LOCK_NAME = "rescore_brand_safety_videos"

@celery_app.task
def rescore_brand_safety_videos(vid_ids=[]):
    if not vid_ids:
        logger.info("No video ids to rescore.")
        try:
            unlock(RESCORE_LOCK_NAME)
        except Exception:
            pass
        return
    try:
        lock(lock_name=RESCORE_LOCK_NAME, max_retries=1, expire=TaskExpiration.CUSTOM_TRANSCRIPTS)
    except Exception as e:
        pass
    logger.info("Rescoring brand safety for videos with new Watson transcripts.")
    auditor = BrandSafetyAudit()
    auditor.process_videos(vid_ids)
    logger.info("Finished rescoring brand safety for videos with new Watson transcripts.")
    unlock(RESCORE_LOCK_NAME)
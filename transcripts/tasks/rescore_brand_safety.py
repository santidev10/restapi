import logging
from saas import celery_app
from brand_safety.auditors.brand_safety_audit import BrandSafetyAudit

logger = logging.getLogger(__name__)

@celery_app.task
def rescore_brand_safety_videos(vid_ids=[]):
    auditor = BrandSafetyAudit()
    auditor.process_videos(vid_ids)

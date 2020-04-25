from brand_safety.auditors.brand_safety_audit import BrandSafetyAudit
from saas import celery_app


@celery_app.task
def channel_update(channel_ids, ignore_vetted_channels=True, ignore_vetted_videos=True):
    if channel_ids is not None:
        if isinstance(channel_ids, str):
            channel_ids = [channel_ids]
        auditor = BrandSafetyAudit(ignore_vetted_channels=ignore_vetted_channels, ignore_vetted_videos=ignore_vetted_videos)
        auditor.process_channels(channel_ids)

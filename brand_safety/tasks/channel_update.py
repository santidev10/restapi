from brand_safety.auditors.brand_safety_audit import BrandSafetyAudit
from saas import celery_app


@celery_app.task
def channel_update(channel_ids):
    if channel_ids is not None:
        if type(channel_ids) is str:
            channel_ids = [channel_ids]
        auditor = BrandSafetyAudit()
        auditor.process_channels(channel_ids)

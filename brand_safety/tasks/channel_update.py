from brand_safety.auditors.channel_auditor import ChannelAuditor
from saas import celery_app


@celery_app.task
def channel_update(channel_ids):
    if channel_ids is not None:
        if isinstance(channel_ids, str):
            channel_ids = [channel_ids]
        auditor = ChannelAuditor()
        auditor.process(channel_ids)

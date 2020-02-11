from audit_tool.models import AuditProcessor

from saas import celery_app
from segment.models import CustomSegment


LOCK_NAME = "segment.update_segment_statistics"


@celery_app.task(expires=60 * 5, soft_time_limit=60 * 5)
def update_segment_statistics():
    """
    Updates segment statistics values
    :return:
    """
    vetting_audits_in_progress = AuditProcessor.objects.filter(
        completed__isnull=True,
        source=1,
    )
    for audit in vetting_audits_in_progress:
        try:
            segment = CustomSegment.objects.get(audit_id=audit.id)
        except CustomSegment.DoesNotExist:
            continue
        vetting_model = segment.audit_utils.vetting_model
        all_vetting_items = vetting_model.objects.filter(audit=audit)
        vetted_items_count = all_vetting_items.filter(processed__isnull=False).count()
        segment.statistics.update({
            "unvetted_items_count": all_vetting_items.count() - vetted_items_count,
            "vetted_items_count": vetted_items_count
        })
        segment.save(update_fields=["statistics"])

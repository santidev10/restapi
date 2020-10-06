from audit_tool.models import AuditProcessor

from segment.models import CustomSegment


def segment_audit_scheduler_task():
    audit_meta_processors = AuditProcessor.objects\
        .filter(source=0, completed__isnull=False, params__segment_id__isnull=False)
    for audit in audit_meta_processors:
        # initiate finalize
        finalize_segment_audit.delay(audit.params["segment_id"])


from saas import celery_app


@celery_app.task
def finalize_segment_audit(segment_id):
    segment = CustomSegment.objects.get(id=segment_id)
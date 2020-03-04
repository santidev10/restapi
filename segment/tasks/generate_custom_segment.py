import logging

from django.utils import timezone

from saas import celery_app
from segment.models import CustomSegment
from segment.tasks.generate_segment import generate_segment
from segment.utils.send_export_email import send_export_email

logger = logging.getLogger(__name__)


@celery_app.task
def generate_custom_segment(segment_id):
    try:
        segment = CustomSegment.objects.get(id=segment_id)
        export = segment.export
        results = generate_segment(segment, export.query["body"], segment.LIST_SIZE, add_uuid=False)
        segment.statistics = results["statistics"]
        export.download_url = results["download_url"]
        export.completed_at = timezone.now()
        export.save()
        segment.save()
        export.refresh_from_db()
        send_export_email(segment.owner.email, segment.title, export.download_url)
        logger.info(f"Successfully generated export for custom list: id: {segment.id}, title: {segment.title}")
    except Exception:
        logger.exception("Error in generate_custom_segment task")

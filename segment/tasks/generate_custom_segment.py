import logging

from django.db import connections
from django.db.utils import OperationalError
from django.utils import timezone

from saas import celery_app
from segment.models import CustomSegment
from segment.tasks.generate_segment import generate_segment
from segment.utils.send_export_email import send_export_email

logger = logging.getLogger(__name__)


@celery_app.task
def generate_custom_segment(segment_id, results=None, tries=0, with_audit=True):
# pylint: disable=broad-except
    try:
        segment = CustomSegment.objects.get(id=segment_id)
        export = segment.export
        args = (segment, export.query["body"], segment.config.LIST_SIZE)
        # If creating with_audit, do not send results email as export requires further processing with audit_tool logic
        if with_audit is True:
            generate_segment(*args, with_audit=with_audit)
            return
        elif results is None:
            results = generate_segment(*args, segment.config.LIST_SIZE)
        segment.statistics = results["statistics"]
        export.download_url = results["download_url"]
        export.completed_at = timezone.now()
        export.filename = results["s3_key"]
        export.save()
        segment.save()
        export.refresh_from_db()
        send_export_email(segment.owner.email, segment.title, export.download_url)
        logger.info("Successfully generated export for custom list: id: %s, title: %s", segment.id, segment.title)
    except OperationalError as e:
        if tries < 2:
            tries += 1
            default_connection = connections["default"]
            default_connection.connect()
            generate_custom_segment(segment_id, results=results, tries=tries)
        else:
            raise e
    except Exception:
        logger.exception("Error in generate_custom_segment task")
# pylint: enable=broad-except

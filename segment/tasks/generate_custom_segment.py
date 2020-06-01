from django.db import connections
from django.db.utils import OperationalError
from django.utils import timezone
from saas import celery_app
from segment.models import CustomSegment
from segment.tasks.generate_segment import generate_segment
from segment.tasks.generate_with_source_list import generate_with_source
from segment.utils.send_export_email import send_export_email
import logging

logger = logging.getLogger(__name__)


@celery_app.task
def generate_custom_segment(segment_id, results=None, tries=0, with_source=False):
    try:
        segment = CustomSegment.objects.get(id=segment_id)
        export = segment.export
        if results is None:
            pass
            if with_source:
            # if segment.source_list:
                results = generate_with_source(segment, None)
            else:
                results = generate_segment(segment, export.query["body"], segment.LIST_SIZE, add_uuid=False)
        results = generate_segment(segment, export.query["body"], segment.LIST_SIZE, add_uuid=False)
        segment.statistics = results["statistics"]
        export.download_url = results["download_url"]
        export.completed_at = timezone.now()
        export.save()
        segment.save()
        export.refresh_from_db()
        send_export_email(segment.owner.email, segment.title, export.download_url)
        logger.info(f"Successfully generated export for custom list: id: {segment.id}, title: {segment.title}")
    except OperationalError as e:
        if tries < 2:
            tries += 1
            default_connection = connections['default']
            default_connection.connect()
            generate_custom_segment(segment_id, results=results, tries=tries)
        else:
            raise e
    except Exception as e:
        logger.exception("Error in generate_custom_segment task")

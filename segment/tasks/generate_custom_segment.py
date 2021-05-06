import logging

from django.db import connections
from django.db.utils import OperationalError
from django.utils import timezone

from saas import celery_app
from segment.models import CustomSegment
from segment.utils.generate_segment import generate_segment
from segment.utils.generate_segment import CTLGenerateException
from segment.utils.send_export_email import send_export_email
from userprofile.constants import StaticPermissions

logger = logging.getLogger(__name__)


@celery_app.task
def generate_custom_segment(segment_id, results=None, tries=0, with_audit=False):
    error_message = None
    segment = CustomSegment.objects.get(id=segment_id)
    try:
        export = segment.export
        size = segment.config.ADMIN_LIST_SIZE
        args = (segment, export.query["body"], size)
        results = generate_segment(*args, with_audit=with_audit)
        segment.statistics.update(results.get("statistics", {}))
        export.download_url = results.get("download_url")
        export.completed_at = timezone.now()
        export.filename = results.get("s3_key")
        export.admin_filename = results.get("admin_s3_key")
        export.save()
        segment.save()
        export.refresh_from_db()
        # If creating with_audit, do not send results email as export requires further processing with audit_tool logic
        if with_audit is False:
            owner = segment.owner
            # email admin version export if user is staff
            if owner.has_permission(StaticPermissions.BUILD__CTL_EXPORT_ADMIN):
                send_export_email(segment.owner.email, segment.title, export.download_url)
                logger.info("Successfully generated export for custom list: id: %s, title: %s", segment.id,
                            segment.title)
            else:
                s3_key = segment.get_s3_key()
                download_url = segment.s3.generate_temporary_url(s3_key, time_limit=3600 * 24 * 7)
                send_export_email(segment.owner.email, segment.title, download_url)
                logger.info("Successfully generated export for custom list: id: %s, title: %s", segment.id,
                            segment.title)

    except OperationalError as e:
        if tries < 2:
            tries += 1
            default_connection = connections["default"]
            default_connection.connect()
            generate_custom_segment(segment_id, results=results, tries=tries)
        else:
            raise e

    except CTLGenerateException as e:
        logger.exception(f"Error in generate_segment with id: {segment_id}")
        error_message = e.message
    except Exception:
        logger.exception(f"Uncaught error in generate_custom_segment task with id: {segment_id}")
        error_message = "Unable to generate export"
    finally:
        if error_message:
            segment.statistics["error"] = error_message
            segment.save(update_fields=["statistics"])

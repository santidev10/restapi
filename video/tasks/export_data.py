import logging

from django.conf import settings

from administration.notifications import send_email
from saas import celery_app
from utils.aws.export_context_manager import ExportContextManager
from utils.es_components_exporter import ESDataS3Exporter
from video.constants import VIDEO_CSV_HEADERS
from video.tasks.export_generator import VideoListDataGenerator

logger = logging.getLogger(__name__)


@celery_app.task
def export_videos_data(query_params, export_name, user_emails):
    content_exporter = ExportContextManager(
        VideoListDataGenerator(query_params),
        VIDEO_CSV_HEADERS
    )
    ESDataS3Exporter.export_to_s3(content_exporter, export_name)

    export_url = ESDataS3Exporter.generate_temporary_url(ESDataS3Exporter.get_s3_key(export_name), time_limit=86400)

    # prepare E-mail
    subject = "Export Videos"
    body = f"Export is ready to download.\n" \
           f"Please click <a href='{export_url}'>here</a> to download the report.\n" \
           f"NOTE: Link will expire in 24 hours.\n"

    # E-mail
    from_email = settings.EXPORTS_EMAIL_ADDRESS

    if send_email(subject=subject, message=None, from_email=from_email, recipient_list=user_emails, html_message=body):
        logger.info("RESEARCH EXPORT: Email was sent to %s.", user_emails)

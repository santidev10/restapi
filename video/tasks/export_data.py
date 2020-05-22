import logging

from saas import celery_app
from django.conf import settings
from django.core.mail import send_mail

from es_components.constants import Sections
from es_components.managers import VideoManager
from utils.es_components_api_utils import BrandSafetyParamAdapter
from utils.es_components_api_utils import ExportDataGenerator
from utils.es_components_api_utils import ESQuerysetAdapter
from utils.es_components_exporter import ESDataS3Exporter
from utils.aws.export_context_manager import ExportContextManager
from video.constants import TERMS_FILTER
from video.constants import MATCH_PHRASE_FILTER
from video.constants import RANGE_FILTER
from video.constants import EXISTS_FILTER
from video.constants import VIDEO_CSV_HEADERS
from video.api.serializers.video_export import VideoListExportSerializer

logger = logging.getLogger(__name__)


class VideoListDataGenerator(ExportDataGenerator):
    serializer_class = VideoListExportSerializer
    terms_filter = TERMS_FILTER
    range_filter = RANGE_FILTER
    match_phrase_filter = MATCH_PHRASE_FILTER
    exists_filter = EXISTS_FILTER
    params_adapters = (BrandSafetyParamAdapter,)
    queryset = ESQuerysetAdapter(VideoManager((
        Sections.MAIN,
        Sections.GENERAL_DATA,
        Sections.STATS,
        Sections.ADS_STATS,
        Sections.BRAND_SAFETY,
    )))


@celery_app.task
def export_videos_data(query_params, export_name, user_emails, export_url):
    content_exporter = ExportContextManager(
        VideoListDataGenerator(query_params),
        VIDEO_CSV_HEADERS
    )
    ESDataS3Exporter.export_to_s3(content_exporter, export_name)

    # prepare E-mail
    subject = "Export Videos"
    body = f"Export is ready to download.\n" \
           f"Please click <a href='{export_url}'>here</a> to download the report.\n" \
           f"NOTE: Link will expire in 2 weeks.\n"

    # E-mail
    from_email = settings.EXPORTS_EMAIL_ADDRESS
    bcc = []

    try:
        send_mail(subject=subject, message=None, from_email=from_email, recipient_list=user_emails, html_message=body)
    except Exception as e:
        logger.info(f"RESEARCH EXPORT: Error during sending email to {user_emails}: {e}")
    else:
        logger.info(f"RESEARCH EXPORT: Email was sent to {user_emails}.")

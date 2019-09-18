from saas import celery_app
from django.urls import reverse
from django.conf import settings
from django.core.mail import EmailMessage

from es_components.constants import Sections
from es_components.managers import VideoManager
from highlights.api.views.videos import ORDERING_FIELDS
from highlights.api.views.videos import TERMS_FILTER
from highlights.api.urls.names import HighlightsNames
from utils.es_components_api_utils import ExportDataGenerator
from utils.es_components_api_utils import ESQuerysetAdapter
from utils.es_components_exporter import ESDataS3Exporter
from utils.aws.export_context_manager import ExportContextManager
from video.api.serializers.video_export import VideoListExportSerializer
from video.constants import VIDEO_CSV_HEADERS
from saas.urls.namespaces import Namespace


class HighlightsVideoListDataGenerator(ExportDataGenerator):
    serializer_class = VideoListExportSerializer
    ordering_fields = ORDERING_FIELDS
    terms_filter = TERMS_FILTER
    queryset = ESQuerysetAdapter(
        VideoManager(
            (
                Sections.MAIN,
                Sections.GENERAL_DATA,
                Sections.STATS,
                Sections.ADS_STATS,
                Sections.BRAND_SAFETY,)
        )
    )\
        .order_by(ORDERING_FIELDS[0])\
        .with_limit(100)



@celery_app.task
def export_videos_data(query_params, export_name, user_emails):
    content_exporter = ExportContextManager(
        HighlightsVideoListDataGenerator(query_params),
        VIDEO_CSV_HEADERS
    )
    ESDataS3Exporter.export_to_s3(content_exporter, export_name)

    url_to_export = reverse(
        "{}:{}".format(Namespace.HIGHLIGHTS,  HighlightsNames.VIDEOS_EXPORT),
        args=(export_name,)
    )

    # prepare E-mail
    subject = "Export Videos"
    body = f"File is ready for downloading.\n" \
           f"Please, go to {url_to_export} to download the report.\n" \
           f"NOTE: url to download report is valid during next 2 weeks\n"

    # E-mail
    from_email = settings.SENDER_EMAIL_ADDRESS
    bcc = []

    email = EmailMessage(
        subject=subject,
        body=body,
        from_email=from_email,
        to=user_emails,
        bcc=bcc,
    )
    email.send(fail_silently=False)
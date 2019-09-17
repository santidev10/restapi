from saas import celery_app
from django.urls import reverse
from django.conf import settings
from django.core.mail import EmailMessage

from es_components.constants import Sections
from es_components.managers import VideoManager
from utils.es_components_api_utils import ExportDataGenerator
from utils.es_components_api_utils import ESQuerysetAdapter
from utils.aws.export_context_manager import ExportContextManager
from video.api.views.video_list import EXISTS_FILTER
from video.api.views.video_list import MATCH_PHRASE_FILTER
from video.api.views.video_list import RANGE_FILTER
from video.api.views.video_list import TERMS_FILTER
from video.api.serializers.video_export import VideoListExportSerializer
from video.api.urls.names import Name
from video.utils import VideoListS3Exporter
from saas.urls.namespaces import Namespace

video_csv_headers = [
    "title",
    "url",
    "views",
    "likes",
    "dislikes",
    "comments",
    "youtube_published_at",
    "brand_safety_score",
    "video_view_rate",
    "ctr",
    "ctr_v",
    "average_cpv",
]


class VideoListDataGenerator(ExportDataGenerator):
    serializer_class = VideoListExportSerializer
    terms_filter = TERMS_FILTER
    range_filter = RANGE_FILTER
    match_phrase_filter = MATCH_PHRASE_FILTER
    exists_filter = EXISTS_FILTER
    queryset = ESQuerysetAdapter(VideoManager((
        Sections.MAIN,
        Sections.GENERAL_DATA,
        Sections.STATS,
        Sections.ADS_STATS,
        Sections.BRAND_SAFETY,
    )))


@celery_app.task
def export_videos_data(query_params, export_name, user_emails):
    content_exporter = ExportContextManager(
        VideoListDataGenerator(query_params),
        video_csv_headers
    )
    VideoListS3Exporter.export_to_s3(content_exporter, export_name)

    url_to_export = reverse("{}:{}".format(Namespace.VIDEO,  Name.VIDEO_EXPORT), args=(export_name,))

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
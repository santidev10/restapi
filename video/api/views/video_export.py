from django.urls import reverse
from django.conf import settings
from django.core.mail import EmailMessage

from rest_framework.fields import CharField
from rest_framework.fields import DateTimeField
from rest_framework.fields import FloatField
from rest_framework.fields import IntegerField
from rest_framework.permissions import IsAdminUser
from rest_framework.serializers import Serializer

from es_components.constants import Sections
from es_components.managers import VideoManager
from utils.api.s3_export_api import S3ExportApiView
from utils.es_components_api_utils import ExportDataGenerator
from utils.es_components_api_utils import ESQuerysetAdapter
from utils.permissions import or_permission_classes
from utils.permissions import user_has_permission
from video.api.views.video_list import EXISTS_FILTER
from video.api.views.video_list import MATCH_PHRASE_FILTER
from video.api.views.video_list import RANGE_FILTER
from video.api.views.video_list import TERMS_FILTER
from video.api.urls.names import Name

from utils.aws.s3_exporter import S3Exporter

S3_EXPORT_KEY_PATTERN = "export/{name}.csv"

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


class YTVideoLinkFromID(CharField):
    def to_representation(self, value):
        str_value = super(YTVideoLinkFromID, self).to_representation(value)
        return f"https://www.youtube.com/watch?v={str_value}"


class VideoListExportSerializer(Serializer):
    title = CharField(source="general_data.title")
    url = YTVideoLinkFromID(source="main.id")
    views = IntegerField(source="stats.views")
    likes = IntegerField(source="stats.likes")
    dislikes = IntegerField(source="stats.dislikes")
    comments = IntegerField(source="stats.comments")
    youtube_published_at = DateTimeField(source="general_data.youtube_published_at")
    brand_safety_score = IntegerField(source="brand_safety.overall_score")
    video_view_rate = FloatField(source="ads_stats.video_view_rate")
    ctr = FloatField(source="ads_stats.ctr")
    ctr_v = FloatField(source="ads_stats.ctr_v")
    average_cpv = FloatField(source="ads_stats.average_cpv")


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


class VideoListS3Exporter(S3Exporter):
    bucket_name = settings.AMAZON_S3_REPORTS_BUCKET_NAME
    export_content_type = "application/CSV"

    @staticmethod
    def get_s3_key(name):
        key = S3_EXPORT_KEY_PATTERN.format(name=name)
        return key



def export_videos_data(query_params, export_name, user_emails):
    content_exporter = ExportDataGenerator(query_params, video_csv_headers)
    VideoListS3Exporter.export_to_s3(content_exporter, export_name)

    url_to_export = reverse("{}:{}".format("video", Name.VIDEO_EXPORT), args=(export_name,))

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


class VideoListExportApiView(S3ExportApiView):
    permission_classes = (
        or_permission_classes(
            user_has_permission("userprofile.video_list"),
            IsAdminUser
        ),
    )
    s3_exporter = VideoListS3Exporter
    generate_export_task = export_videos_data

    @staticmethod
    def get_filename(name):
        return f"Videos export report {name}.csv"

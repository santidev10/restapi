from rest_framework.views import APIView
from rest_framework.permissions import IsAdminUser

from video.utils import VideoListS3Exporter
from video.tasks import export_videos_data
from utils.api.s3_export_api import S3ExportApiView
from utils.permissions import or_permission_classes
from utils.permissions import user_has_permission

# S3_EXPORT_KEY_PATTERN = "export/{name}.csv"
#
# video_csv_headers = [
#     "title",
#     "url",
#     "views",
#     "likes",
#     "dislikes",
#     "comments",
#     "youtube_published_at",
#     "brand_safety_score",
#     "video_view_rate",
#     "ctr",
#     "ctr_v",
#     "average_cpv",
# ]
#
#
# class YTVideoLinkFromID(CharField):
#     def to_representation(self, value):
#         str_value = super(YTVideoLinkFromID, self).to_representation(value)
#         return f"https://www.youtube.com/watch?v={str_value}"
#
#
# class VideoListExportSerializer(Serializer):
#     title = CharField(source="general_data.title")
#     url = YTVideoLinkFromID(source="main.id")
#     views = IntegerField(source="stats.views")
#     likes = IntegerField(source="stats.likes")
#     dislikes = IntegerField(source="stats.dislikes")
#     comments = IntegerField(source="stats.comments")
#     youtube_published_at = DateTimeField(source="general_data.youtube_published_at")
#     brand_safety_score = IntegerField(source="brand_safety.overall_score")
#     video_view_rate = FloatField(source="ads_stats.video_view_rate")
#     ctr = FloatField(source="ads_stats.ctr")
#     ctr_v = FloatField(source="ads_stats.ctr_v")
#     average_cpv = FloatField(source="ads_stats.average_cpv")
#
#
# class VideoListDataGenerator(ExportDataGenerator):
#     serializer_class = VideoListExportSerializer
#     terms_filter = TERMS_FILTER
#     range_filter = RANGE_FILTER
#     match_phrase_filter = MATCH_PHRASE_FILTER
#     exists_filter = EXISTS_FILTER
#     queryset = ESQuerysetAdapter(VideoManager((
#         Sections.MAIN,
#         Sections.GENERAL_DATA,
#         Sections.STATS,
#         Sections.ADS_STATS,
#         Sections.BRAND_SAFETY,
#     )))
#
#
# class VideoListS3Exporter(S3Exporter):
#     bucket_name = settings.AMAZON_S3_REPORTS_BUCKET_NAME
#     export_content_type = "application/CSV"
#
#     @staticmethod
#     def get_s3_key(name):
#         key = S3_EXPORT_KEY_PATTERN.format(name=name)
#         return key



class VideoListExportApiView(S3ExportApiView, APIView):
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

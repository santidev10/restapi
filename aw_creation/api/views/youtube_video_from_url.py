import re

from googleapiclient.discovery import build
from django.conf import settings
from drf_yasg import openapi
from drf_yasg.utils import swagger_auto_schema
from rest_framework.response import Response
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_400_BAD_REQUEST
from rest_framework.status import HTTP_404_NOT_FOUND

from .schemas import VIDEO_FORMAT_PARAMETER
from .schemas import VIDEO_ITEM_SCHEMA
from .youtube_video_search import YoutubeVideoSearchApiView
from userprofile.constants import StaticPermissions


class YoutubeVideoFromUrlApiView(YoutubeVideoSearchApiView):
    permission_classes = (StaticPermissions.has_perms(StaticPermissions.MEDIA_BUYING),)
    url_regex = r"^(?:https?:/{1,2})?(?:w{3}\.)?youtu(?:be)?\.(?:com|be)(?:/watch\?v=|/video/|/)([^\s&/\?]+)(?:.*)$"

    @swagger_auto_schema(
        manual_parameters=[
            openapi.Parameter(
                name="url",
                required=True,
                in_=openapi.IN_PATH,
                description="urlencoded Youtube video URL",
                type=openapi.TYPE_STRING,
            ),
            VIDEO_FORMAT_PARAMETER,
        ],
        responses={
            HTTP_200_OK: VIDEO_ITEM_SCHEMA,
            HTTP_400_BAD_REQUEST: openapi.Response("Wrong request parameters"),
            HTTP_404_NOT_FOUND: openapi.Response("Video not found"),
        }
    )
    def get(self, request, url, **_):
        match = re.match(self.url_regex, url)
        if match:
            yt_id = match.group(1)
        else:
            return Response(status=HTTP_400_BAD_REQUEST,
                            data=dict(error="Wrong url format"))

        video_ad_format = request.GET.get("video_ad_format")

        youtube = build(
            "youtube", "v3",
            developerKey=settings.YOUTUBE_API_DEVELOPER_KEY
        )
        options = {
            "id": yt_id,
            "part": "snippet,contentDetails",
            "maxResults": 1,
        }
        results = youtube.videos().list(**options).execute()
        items = results.get("items", [])
        if not items:
            return Response(status=HTTP_404_NOT_FOUND,
                            data=dict(error="There is no such a video"))

        item = self.format_item(items[0])
        if video_ad_format == "BUMPER" \
            and item["duration"] \
            and item["duration"] > 6:
            return Response(status=HTTP_404_NOT_FOUND,
                            data=dict(error="There is no such a Bumper ads video (<= 6 seconds)"))

        return Response(data=item)

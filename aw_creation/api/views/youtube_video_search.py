import isodate
from googleapiclient.discovery import build
from django.conf import settings
from drf_yasg import openapi
from drf_yasg.utils import swagger_auto_schema
from rest_framework.generics import GenericAPIView
from rest_framework.response import Response
from rest_framework.status import HTTP_200_OK

from .schemas import VIDEO_FORMAT_PARAMETER
from .schemas import VIDEO_RESPONSE_SCHEMA
from userprofile.constants import StaticPermissions


class YoutubeVideoSearchApiView(GenericAPIView):
    permission_classes = (StaticPermissions()(StaticPermissions.MEDIA_BUYING),)

    @swagger_auto_schema(
        manual_parameters=[
            openapi.Parameter(
                name="query",
                required=True,
                in_=openapi.IN_PATH,
                description="urlencoded search string to lookup Youtube videos",
                type=openapi.TYPE_STRING,
            ),
            VIDEO_FORMAT_PARAMETER,
        ],
        responses={
            HTTP_200_OK: VIDEO_RESPONSE_SCHEMA
        }
    )
    def get(self, request, query, **_):
        video_ad_format = request.GET.get("video_ad_format")

        youtube = build(
            "youtube", "v3",
            developerKey=settings.YOUTUBE_API_DEVELOPER_KEY
        )
        next_page = self.request.GET.get("next_page")

        items, next_token, total_result = self.search_yt_videos(youtube, query,
                                                                next_page)

        if video_ad_format == "BUMPER":
            while len(items) < 10 and next_token:
                add_items, next_token, total_result = self.search_yt_videos(
                    youtube, query, next_token)
                items.extend(add_items)

        response = dict(next_page=next_token, items_count=total_result,
                        items=items)
        return Response(data=response)

    def search_yt_videos(self, youtube, query, next_page):

        video_ad_format = self.request.GET.get("video_ad_format")
        options = {
            "q": query,
            "part": "id",
            "type": "video",
            "videoDuration": "short" if video_ad_format == "BUMPER" else "any",
            "maxResults": 50,
            "safeSearch": "none",
            "eventType": "completed",
        }
        if next_page:
            options["pageToken"] = next_page
        search_results = youtube.search().list(**options).execute()
        ids = [i.get("id", {}).get("videoId") for i in
               search_results.get("items", [])]

        results = youtube.videos().list(part="snippet,contentDetails",
                                        id=",".join(ids)).execute()
        items = [self.format_item(i) for i in results.get("items", [])]

        if video_ad_format == "BUMPER":
            items = list(
                filter(lambda i: i["duration"] and i["duration"] <= 6, items))

        return items, search_results.get("nextPageToken"), search_results.get(
            "pageInfo", {}).get("totalResults")

    @staticmethod
    def format_item(data):
        snippet = data.get("snippet", {})
        thumbnails = snippet.get("thumbnails", {})
        thumbnail = thumbnails.get("high") if "high" in thumbnails \
            else thumbnails.get("default")
        uid = data.get("id", {})
        if isinstance(uid, dict):
            uid = uid.get("videoId")
        item = dict(
            id=uid,
            title=snippet.get("title"),
            url="https://youtube.com/video/{}".format(uid),
            description=snippet.get("description"),
            thumbnail=thumbnail.get("url"),
            channel=dict(
                id=snippet.get("channelId"),
                title=snippet.get("channelTitle"),
            ),
            duration=isodate.parse_duration(
                data["contentDetails"]["duration"]).total_seconds(),
        )
        return item

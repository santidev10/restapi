"""
Channel api views module
"""
import re
import time
from copy import deepcopy
from datetime import datetime

import requests
from dateutil import parser
from django.contrib.auth.mixins import PermissionRequiredMixin
from django.db.models import Q
from django.http import QueryDict
from rest_framework.response import Response
from rest_framework.status import HTTP_400_BAD_REQUEST, \
    HTTP_408_REQUEST_TIMEOUT, HTTP_404_NOT_FOUND, HTTP_412_PRECONDITION_FAILED
from rest_framework.views import APIView

from channel.api.mixins import ChannelYoutubeSearchMixin
from segment.models import SegmentChannel
# pylint: disable=import-error
from singledb.api.views.base import SingledbApiView
from singledb.connector import IQApiConnector as IQConnector
from singledb.connector import SingleDatabaseApiConnector as Connector, \
    SingleDatabaseApiConnectorException
from userprofile.models import UserChannel
from utils.csv_export import CassandraExportMixin
from utils.permissions import OnlyAdminUserCanCreateUpdateDelete


# pylint: enable=import-error


class ChannelListApiView(
        APIView,
        PermissionRequiredMixin,
        CassandraExportMixin,
        ChannelYoutubeSearchMixin):
    """
    Proxy view for channel list
    """
    # TODO disable debug
    permission_classes = tuple()
    # permission_required = (
    #     "userprofile.channel_list",
    #     "userprofile.settings_my_yt_channels"
    # )

    fields_to_export = [
        "title",
        "url",
        "country",
        "category",
        "emails",
        "description",
        "subscribers",
        "thirty_days_subscribers",
        "thirty_days_views",
        "views_per_video",
        "sentiment",
        "engage_rate",
        "last_video_published_at"
    ]
    export_file_title = "channel"

    def obtain_segment(self, segment_id):
        """
        Try to get segment from db
        """
        try:
            if self.request.user.is_staff:
                segment = SegmentChannel.objects.get(id=segment_id)
            else:
                segment = SegmentChannel.objects.filter(
                    Q(owner=self.request.user) |
                    ~Q(category="private")).get(id=segment_id)
        except SegmentChannel.DoesNotExist:
            return None
        return segment

    def get(self, request):
        """
        Get procedure
        """
        empty_response = {
            "max_page": 1,
            "items_count": 0,
            "items": [],
            "current_page": 1,
        }

        if any((
                request.query_params.get("youtube_link"),
                request.query_params.get("youtube_keyword"))):
            return self.search_channels()
        # prepare query params
        query_params = deepcopy(request.query_params)
        query_params._mutable = True

        # segment
        segment = query_params.get("segment")
        if segment is not None:
            # obtain segment
            segment = self.obtain_segment(segment)
            if segment is None:
                return Response(status=HTTP_404_NOT_FOUND)
            # obtain channels ids
            channels_ids = segment.get_related_ids()
            if not channels_ids:
                return Response(empty_response)
            query_params.pop("segment")
            query_params.update(ids=",".join(channels_ids))

        # own_channels
        if not request.user.has_perm("userprofile.channel_list") and \
           request.user.has_perm("userprofile.settings_my_yt_channels"):
            own_channels = "1"
        else:
            own_channels = query_params.get("own_channels", "0")

        if query_params.get("own_channels") is not None:
            query_params.pop("own_channels")

        if own_channels == "1":
            user = self.request.user
            if not user or not user.is_authenticated():
                return Response(status=HTTP_412_PRECONDITION_FAILED)
            channels_ids = user.channels.values_list("channel_id", flat=True)
            if not channels_ids:
                return Response(empty_response)
            query_params.update(ids=",".join(channels_ids))
            query_params.update(timestamp=str(time.time()))

        # adapt the request params
        self.adapt_query_params(query_params)

        # make call
        connector = Connector()
        try:
            response_data = connector.get_channel_list(query_params)
        except SingleDatabaseApiConnectorException as e:
            return Response(
                data={"error": " ".join(e.args)},
                status=HTTP_408_REQUEST_TIMEOUT)

        # hide data according to user permissions
        items = response_data.get("items", [])
        for item in items:
            if not self.request.user.has_perm('userprofile.channel_audience') and \
               not (own_channels == '1' and item['channel_id'] in channels_ids):
                item["has_audience"] = False

        # adapt the response data
        self.adapt_response_data(response_data)

        return Response(response_data)

    @staticmethod
    def adapt_query_params(query_params):
        """
        Adapt SDB request format
        """
        # filters --->
        def make_range(name, name_min=None, name_max=None):
            if name_min is None:
                name_min = "min_{}".format(name)
            if name_max is None:
                name_max = "max_{}".format(name)
            _range = [
                query_params.pop(name_min, [None])[0],
                query_params.pop(name_max, [None])[0],
            ]
            _range = [str(v) if v is not None else "" for v in _range]
            _range = ",".join(_range)
            if _range != ",":
                query_params.update(**{"{}__range".format(name): _range})

        def make(_type, name, name_in=None):
            if name_in is None:
                name_in = name
            value = query_params.pop(name_in, [None])[0]
            if value is not None:
                query_params.update(**{"{}__{}".format(name, _type): value})

        # min_subscribers_yt, max_subscribers_yt
        make_range("subscribers", "min_subscribers_yt", "max_subscribers_yt")

        # country
        make("terms", "country")

        # language
        make("terms", "language")

        # min_thirty_days_subscribers, max_thirty_days_subscribers
        make_range("thirty_days_subscribers")

        # min_thirty_days_views, max_thirty_days_views
        make_range("thirty_days_views")

        # min_sentiment, max_sentiment
        make_range("sentiment")

        # min_engage_rate, max_engage_rate
        make_range("engage_rate")

        # min_views_per_video, max_views_per_video
        make_range("views_per_video")

        # min_subscribers_fb, max_subscribers_fb
        make_range(
            "facebook_likes", "min_subscribers_fb", "max_subscribers_fb")

        # min_subscribers_tw, max_subscribers_tw
        make_range(
            "twitter_followers", "min_subscribers_tw", "max_subscribers_tw")

        # min_subscribers_in, max_subscribers_in
        make_range(
            "instagram_followers", "min_subscribers_in", "max_subscribers_in")

        # category
        category = query_params.pop("category", [None])[0]
        if category is not None:
            regexp = "|".join([".*" + c + ".*" for c in category.split(",")])
            query_params.update(category__regexp=regexp)

        # verified
        verified = query_params.pop("verified", [None])[0]
        if verified is not None:
            query_params.update(
                has_audience__term="false" if verified == "0" else "true")

        # text_search
        text_search = query_params.pop("text_search", [None])[0]
        if text_search:
            words = [s.lower() for s in re.split(r"\s+", text_search) if s and not re.match('^\W+$', s)]
            if words:
                query_params.update(text_search__term=words)

        # channel_group
        make("term", "channel_group")
        # <--- filters

    @staticmethod
    def adapt_response_data(response_data):
        """
        Adapt SDB response format
        """
        items = response_data.get("items", [])
        for item in items:
            if "channel_id" in item: 
                item["id"] = item.get("channel_id", "")
                del item["channel_id"]
            if "country" in item and item["country"] is None:
                item["country"] = ""
            if "history_date" in item and item["history_date"]:
                item["history_date"] = item["history_date"][:10]
            if "has_audience" in item:
                item["verified"] = item["has_audience"]
            for field in ["youtube_published_at", "updated_at"]:
                if field in item and item[field]:
                    item[field] = re.sub(
                        "^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+|)$",
                        "\g<0>Z",
                        item[field]
                    )
        return response_data


class ChannelListFiltersApiView(SingledbApiView):
    permission_required = ('userprofile.channel_filter',)
    connector_get = Connector().get_channel_filters_list


class ChannelRetrieveUpdateApiView(SingledbApiView):
    permission_classes = (OnlyAdminUserCanCreateUpdateDelete,)
    permission_required = ('userprofile.channel_details',)
    connector_get = Connector().get_channel
    connector_put = Connector().put_channel

    def put(self, *args, **kwargs):
        data = self.request.data
        permitted_groups = ["influencers", "new", "media", "brands"]
        if "channel_group" in data and data["channel_group"] not in permitted_groups:
            return Response(status=HTTP_400_BAD_REQUEST)
        response = super().put(*args, **kwargs)
        ChannelListApiView.adapt_response_data({'items': [response.data]})
        return response

    def get(self, *args, **kwargs):
        response = super().get(*args, **kwargs)
        pk = kwargs.get('pk')
        if pk:
            query = QueryDict("channel_id__term={}"
                              "&sort=youtube_published_at:desc"
                              "&size=50"
                              "&fields=video_id"
                                     ",title"
                                     ",thumbnail_image_url"
                                     ",views"
                                     ",youtube_published_at"
                                     ",likes"
                                     ",comments".format(pk))
            videos = Connector().get_video_list(query)['items']
            now = datetime.now()
            average_views = 0
            if len(videos):
                average_views = round(sum([v.get("views", 0) for v in videos]) / len(videos))
            for v in videos:
                v["id"] = v.pop("video_id")
                youtube_published_at = v.pop("youtube_published_at")
                if youtube_published_at:
                    v['days'] = (now - parser.parse(youtube_published_at)).days
            response.data["performance"] = {
                'average_views': average_views,
                'videos': videos,
            }
            channels_ids = self.request.user.channels.values_list("channel_id", flat=True)
            if not self.request.user.has_perm('userprofile.channel_audience') and \
               pk not in channels_ids:
                response.data['has_audience'] = False
                response.data.pop('audience', None)
                response.data.pop('aw_data', None)
                response.data['brand_safety'] = None
                response.data.pop('genre', None)
                response.data['safety_chart_data'] = None
                response.data.pop('traffic_sources', None)
        ChannelListApiView.adapt_response_data({'items': [response.data]})
        return response


class ChannelSetApiView(SingledbApiView):
    permission_classes = (OnlyAdminUserCanCreateUpdateDelete,)
    connector_delete = Connector().delete_channels


class ChannelAuthenticationApiView(APIView):
    def post(self, request, *args, **kwagrs):
        connector = Connector()
        try:
            data = connector.auth_channel(request.data)
        except SingleDatabaseApiConnectorException as e:
            data = {"error": " ".join(e.args)}
            return Response(data=data, status=HTTP_408_REQUEST_TIMEOUT)

        if data is not None:
            channel_id = data.get('channel_id')
            if channel_id:
                user = self.request.user
                if not user or not user.is_authenticated():
                    return Response(status=HTTP_412_PRECONDITION_FAILED)
                user_channels = user.channels.values_list('channel_id', flat=True)
                if channel_id not in user_channels:
                    UserChannel.objects.create(channel_id=channel_id, user=user)
                # set user avatar
                self.set_user_avatar(data.get("access_token"))

        return Response()

    def set_user_avatar(self, access_token):
        """
        Obtain user avatar from google+
        """
        token_info_url = "https://www.googleapis.com/oauth2/v3/tokeninfo" \
                         "?access_token={}".format(access_token)
        # --> obtain token info
        try:
            response = requests.get(token_info_url)
        except Exception:
            return
        if response.status_code != 200:
            return
        # <-- obtain token info
        # --> obtain user from google +
        response = response.json()
        user_google_id = response.get("sub")
        google_plus_api_url = "https://www.googleapis.com/plus/v1/people/{}/" \
                      "?access_token={}".format(user_google_id, access_token)
        try:
            response = requests.get(google_plus_api_url)
        except Exception:
            return
        extra_details = response.json()
        # <-- obtain user from google +
        # --> set user avatar
        if not extra_details.get("image", {}).get("isDefault", True):
            profile_image_url = extra_details.get(
                "image", {}).get("url", "").replace("sz=50", "sz=250")
            self.request.user.profile_image_url = profile_image_url
            self.request.user.save()
        # <-- set user avatar
        return

#
# class ChannelSearchApiView(APIView):
#     """
#     Description temporarily unavailable
#     """
#     permission_classes = tuple
#     channel_rate_view = None
#     youtube_connector = YoutubeAPIConnector()
#     permission_classes = (VerifiedNotInfluencerUserPermission, )
#
#     def get_channels_ids(self, channels):
#         """
#         Collect channels ids
#         """
#         ids = []
#         for channel in channels:
#             if channel.get('id', {}).get('kind') == 'youtube#channel':
#                 ids.append(channel.get("id", {}).get("channelId"))
#         return ids
#
#     def get_channels_details(self, channels_ids):
#         """
#         Collect channels statistic
#         """
#         ids_chunks = list(chunks(channels_ids, 50))
#         channels_info = []
#         for chunk in ids_chunks:
#             channels_ids_string = ",".join(chunk)
#             try:
#                 channels_data = self.youtube_connector.channels_search(
#                     channels_ids_string,
#                     part="id,snippet,statistics").get("items")
#             except Exception as e:
#                 logger.error(e)
#             else:
#                 channels_info = channels_info + channels_data
#         return channels_info
#
#     def get_response_data(self, channels, full_info=False,
#                           next_page_token=None):
#         """
#         Prepare response json
#         """
#         response_data = {
#             "next_page_token": None,
#             "items": None,
#             "items_count": 0
#         }
#         if not channels:
#             return response_data
#         channels_ids = self.get_channels_ids(channels)
#         channels_details = self.get_channels_details(channels_ids)
#         items = []
#         if full_info:
#             for channel in channels_details:
#                 if not self.channel_rate_view:
#                     self.channel_rate_view = ChannelStatisticsApiView()
#                 channel = self.channel_rate_view.get_statistics(
#                     str(channel.get("id"))).data
#                 items.append(channel)
#         else:
#             for channel in channels_details:
#                 channel_snippet = channel.get("snippet", {})
#                 channel_statistics = channel.get("statistics", {})
#                 description = channel_snippet.get(
#                     'description', "No description available")
#                 youtube_id = channel.get("id")
#                 thumbnail_image_url = channel_snippet.get("thumbnails", {}).get(
#                     "default", {}).get("url")
#                 title = channel_snippet.get("title", "No title available")
#                 views = channel_statistics.get("viewCount")
#                 videos = channel_statistics.get("videoCount")
#                 subscribers = channel_statistics.get("subscriberCount")
#                 country = channel_snippet.get("country")
#                 items.append({
#                     "youtube_id": youtube_id,
#                     "thumbnail_image_url": thumbnail_image_url,
#                     "title": title,
#                     "description": description,
#                     "videos_count": videos,
#                     "country": country,
#                     "details": {
#                         "subscribers": subscribers,
#                         "videos_count": videos,
#                         "views": views
#                     }
#                 })
#         response_data["items"] = items
#         response_data["items_count"] = len(channels_details)
#         if next_page_token:
#             response_data["next_page_token"] = next_page_token
#         return response_data
#
#     def keywords_search(self, keywords, next_page_token, full_info=False):
#         """
#         Search channels by topics
#         """
#         try:
#             channels_data = self.youtube_connector.key_words_list_search(
#                 key_words=keywords, part='id', page_token=next_page_token)
#         except QuotaExceededException:
#             logger.error('Youtube API Quota Exceeded')
#             return Response(status=HTTP_503_SERVICE_UNAVAILABLE, data={"error": ["Youtube Data API Quota Exceeded"]})
#         except Exception as e:
#             logger.error(e)
#             return Response(status=HTTP_408_REQUEST_TIMEOUT)
#         channels = channels_data.get("items")
#         next_page_token = channels_data.get("nextPageToken")
#         if full_info:
#             return Response(data=self.get_response_data(
#                 channels, full_info=True, next_page_token=next_page_token))
#         return Response(data=self.get_response_data(
#             channels, next_page_token=next_page_token))
#
#     @cached_view
#     def get(self, request):
#         """
#         Search channels by keywords and return json with channels
#         """
#         keywords = request.query_params.get("keywords")
#         full_info = request.query_params.get('full_info')
#         next_page_token = request.query_params.get("next_page")
#         if not keywords:
#             logger.error("Channels search view has got no keywords")
#             return Response(status=HTTP_400_BAD_REQUEST)
#         if keywords and full_info:
#             return self.keywords_search(keywords, next_page_token,
#                                         full_info=True)
#         return self.keywords_search(
#                 keywords, next_page_token)
#
#
# class ChannelStatisticsApiView(APIView):
#     """
#     Description temporarily unavailable
#     """
#     permission_classes = (VerifiedNotInfluencerUserPermission,)
#
#     def parse_link(self, link):
#         """
#         Get channel id or video channel id from sent link
#         """
#         if type(link) != str:
#             # do not parse non-sting types
#             return
#
#         if "user/" in link:
#             user_id = link.split('user/')[1].split('/')[0]
#             channel_id = YoutubeAPIConnector().user_channel(user_id).get('items', [])
#             if len(channel_id) > 0:
#                 return channel_id[0].get('id')
#             return
#
#         try:
#             channel_id = re.findall(
#                 r"\w+?:?/?/?\w+?.?\w+.\w+/\w+/([0-9a-zA-Z_-]+)/?", link)
#         except TypeError:
#             channel_id = None
#         if channel_id:
#             return channel_id[0]
#
#         try:
#             video_id = re.findall(
#                 r"\w+?:?/?/?\w+?.?\w+.\w+\w+/\w+\?v=([0-9a-zA-Z_-]+)/?", link)
#         except TypeError:
#             video_id = None
#         if not video_id:
#             return
#         return self.get_video_youtube_channel_id(video_id[0])
#
#     def get_video_youtube_channel_id(self, video_id):
#         """
#         Get video channel youtube id procedure
#         """
#         youtube = YoutubeAPIConnector()
#         try:
#             details = youtube.videos_search(
#                 videos_ids="{}".format(video_id),
#                 part="id,statistics,snippet").get("items")[0]
#         except Exception as e:
#             logger.error(e)
#             return None
#         channel_youtube_id = details.get("snippet", {}).get("channelId")
#         return channel_youtube_id
#
#     def parse_videos_info(self, videos_details):
#         """
#         Get parsed videos details
#         """
#         tags = []
#         views = 0
#         likes = 0
#         dislikes = 0
#         comments = 0
#
#         categories = {}
#         selected_category = None
#
#         for details in videos_details:
#             video_tags = details.get("snippet", {}).get("tags", [])
#             tags = tags + video_tags
#             video_views = details.get("statistics", {}).get("viewCount", 0)
#             views += int(video_views)
#             video_likes = details.get("statistics", {}).get("likeCount", 0)
#             likes += int(video_likes)
#             video_dislikes = details.get(
#                 "statistics", {}).get("dislikeCount", 0)
#             dislikes += int(video_dislikes)
#             video_comments = details.get(
#                 "statistics", {}).get("commentCount", 0)
#             comments += int(video_comments)
#             category_id = details.get("snippet", {}).get("categoryId")
#             if category_id:
#                 try:
#                     string_category = VideoCategory.objects.get(
#                         id=category_id).title
#                 except VideoCategory.DoesNotExist:
#                     # categories.add(category_id)
#                     pass
#                 else:
#                     if categories.get(string_category):
#                         categories[string_category] += 1
#                     else:
#                         categories[string_category] = 1
#                     if selected_category is None or categories[selected_category] < categories[string_category]:
#                         selected_category = string_category
#         return {
#             "tags": tags,
#             "views": views,
#             "likes": likes,
#             "dislikes": dislikes,
#             "comments": comments,
#             "videos": len(videos_details),
#             "category": selected_category
#         }
#
#     def parse_videos(self, videos_details):
#         """
#         Get parsed videos
#         """
#         videos = []
#         for detail in videos_details:
#             videos.append({
#                 "title": detail.get("snippet", {}).get("title"),
#                 "thumbnail_image_url": detail.get("snippet", {}).get(
#                     "thumbnails", {}).get("default", {}).get("url"),
#                 "description": detail.get("snippet", {}).get("description"),
#                 "youtube_id": detail.get("id"),
#                 "youtube_published_at": detail.get(
#                     "snippet", {}).get("publishedAt"),
#                 "tags": detail.get("snippet", {}).get("tags"),
#                 "youtube_link": "https://www.youtube.com/watch?v={}".format(
#                     detail.get("id")),
#                 "category": detail.get("snippet", {}).get("categoryId"),
#                 "details": {
#                     "views": detail.get("statistics", {}).get(
#                         "viewCount", 0),
#                     "likes": detail.get("statistics", {}).get(
#                         "likeCount", 0),
#                     "dislikes": detail.get("statistics", {}).get(
#                         "dislikeCount", 0),
#                     "comments": detail.get("statistics", {}).get(
#                         "commentCount", 0)
#                 }
#             })
#         return videos
#
#     @cached_view
#     def get(self, request):
#         """
#         Return extended channel statistics
#         """
#         channel_id = request.query_params.get("channel_id")
#         if channel_id:
#             return self.get_statistics(channel_id)
#         link = request.query_params.get('link')
#         if not link:
#             return Response(status=HTTP_400_BAD_REQUEST)
#         channel_id = self.parse_link(link)
#         if not channel_id:
#             return Response(
#                 {"error": ["Enter a link to a Youtube channel or video"]},
#                 status=HTTP_400_BAD_REQUEST)
#         return self.get_statistics(channel_id)
#
#     def get_statistics(self, youtube_id):
#         """
#         Prepare channel statistics
#         """
#         if not youtube_id:
#             return Response(status=HTTP_400_BAD_REQUEST)
#         cached_data = cache.get(youtube_id)
#         if cached_data:
#             return Response(cached_data)
#         youtube = YoutubeAPIConnector()
#         try:
#             channel_info = youtube.channels_search(
#                 channels_ids=youtube_id).get("items")[0]
#         except Exception as e:
#             logger.error(e)
#             return Response(status=HTTP_408_REQUEST_TIMEOUT)
#         snippet = channel_info.get("snippet", {})
#         content_details = channel_info.get("contentDetails", {})
#         branding_settings = channel_info.get("brandingSettings", {})
#         statistics = channel_info.get("statistics")
#         category = None
#         email = None
#         title = snippet.get("title", "No title available")
#         youtube_published_at = snippet.get("publishedAt")
#         thumbnail_image_url = snippet.get("thumbnails", {}).get(
#             "default", {}).get("url")
#         youtube_link = "https://www.youtube.com/channel/{}".format(youtube_id)
#         description = snippet.get("description", "No description available")
#         content_owner = content_details.get("googlePlusUserId")
#         youtube_keywords = branding_settings.get('channel', {}).get(
#             "keywords", "").split(" ")
#         subscribers = int(statistics.get("subscriberCount", 0))
#         views = int(statistics.get("viewCount", 0))
#         videos_count = int(statistics.get("videoCount", 0))
#         country = snippet.get("country")
#         response_data = {
#             "title": title,
#             "youtube_published_at": youtube_published_at,
#             "thumbnail_image_url": thumbnail_image_url,
#             "youtube_link": youtube_link,
#             "youtube_id": youtube_id,
#             "country": country,
#             "category": category,
#             "content_owner": content_owner,
#             "description": description,
#             "tags": None,
#             "keywords": youtube_keywords,
#             "email": email,
#             "videos_count": videos_count,
#             "videos": None,
#             "details": {
#                 "subscribers": subscribers,
#                 "videos_count": videos_count,
#                 "views": views,
#                 "engagement": None,
#                 "sentiment": None,
#                 "youtube": {
#                     "engagements_per_video": None,
#                     "likes_per_video": None,
#                     "dislikes_per_video": None,
#                     "comments_per_video": None,
#                     "views_per_video": None,
#                 }
#             }
#         }
#         if not videos_count:
#             return Response(response_data)
#         try:
#             videos = youtube.channel_videos_search(channel_id=youtube_id)
#         except Exception as e:
#             logger.error(e)
#             return Response(response_data)
#         videos_ids = [video.get("id", {}).get(
#             "videoId") for video in videos.get("items")]
#         try:
#             videos_details = youtube.videos_search(
#                 videos_ids=",".join(videos_ids),
#                 part="id,statistics,snippet").get("items")
#         except Exception as e:
#             logger.error(e)
#             return Response(response_data)
#         parsed_videos_data = self.parse_videos_info(videos_details)
#         category = parsed_videos_data.get("category")
#         response_data["category"] = category
#         videos = parsed_videos_data.get("videos")
#         videos_views = parsed_videos_data.get("views")
#         views_per_video = videos_views / max(videos, 1)
#         response_data["details"]["youtube"]["views_per_video"] = views_per_video
#         tags = parsed_videos_data.get("tags")
#         response_data["tags"] = tags
#         videos_likes = parsed_videos_data.get("likes")
#         videos_dislikes = parsed_videos_data.get("dislikes")
#         videos_comments = parsed_videos_data.get("comments")
#         total_social_engagements = videos_likes + videos_dislikes\
#                                                 + videos_comments
#         engagements_per_video = total_social_engagements / max(videos, 1)
#         response_data["details"]["youtube"]["engagements_per_video"] = \
#             engagements_per_video
#         likes_per_video = videos_likes / max(videos, 1)
#         response_data["details"]["youtube"]["likes_per_video"] = likes_per_video
#         dislikes_per_video = videos_dislikes / max(videos, 1)
#         response_data["details"]["youtube"]["dislikes_per_video"] = dislikes_per_video
#         comments_per_video = videos_comments / max(videos, 1)
#         response_data["details"]["youtube"]["comments_per_video"] = comments_per_video
#         sentiment = (videos_likes / max(
#             ((videos_likes + videos_dislikes), 1))) * 100
#         response_data["details"]["sentiment"] = sentiment
#         engage_rate = ((videos_likes + videos_dislikes + videos_comments)
#                        / max((videos_views, 1))) * 100
#
#         # SAAS-1042
#         if engage_rate >= 1000:
#             engage_rate = 0.0
#         elif 100 < engage_rate < 1000:
#             engage_rate = 100.00
#
#         response_data["details"]["engagement"] = engage_rate
#         response_data["videos"] = self.parse_videos(videos_details)
#         last_video_published_at = None
#         if videos_details:
#             last_video_published_at = videos_details[0].get(
#                 "snippet").get("publishedAt")
#         response_data["last_video_published_at"] = last_video_published_at
#         cache.set(youtube_id, response_data)
#         return Response(response_data)
#

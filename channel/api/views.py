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
        APIView, PermissionRequiredMixin, CassandraExportMixin):
    """
    Proxy view for channel list
    """
    permission_required = (
        "userprofile.channel_list",
        "userprofile.settings_my_yt_channels"
    )

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
        connector = Connector()
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

            try:
                ids_hash = connector.store_ids(channels_ids)["ids_hash"]
            except SingleDatabaseApiConnectorException as e:
                return Response(
                    data={"error": " ".join(e.args)},
                    status=HTTP_408_REQUEST_TIMEOUT)

            query_params.update(ids_hash=ids_hash)
            query_params.update(timestamp=str(time.time()))

        # adapt the request params
        self.adapt_query_params(query_params)

        # make call
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

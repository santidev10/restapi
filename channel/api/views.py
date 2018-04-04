"""
Channel api views module
"""
import hashlib
import re
from copy import deepcopy
from datetime import datetime

import requests
from dateutil import parser
from django.conf import settings
from django.contrib.auth import get_user_model
from django.contrib.auth.mixins import PermissionRequiredMixin
from django.http import QueryDict
from django.utils import timezone
from rest_framework.authtoken.models import Token
from rest_framework.permissions import BasePermission
from rest_framework.response import Response
from rest_framework.status import HTTP_400_BAD_REQUEST, \
    HTTP_408_REQUEST_TIMEOUT, HTTP_404_NOT_FOUND, HTTP_412_PRECONDITION_FAILED, \
    HTTP_202_ACCEPTED
from rest_framework.views import APIView

from administration.notifications import send_welcome_email, \
    send_new_channel_authentication_email
from channel.api.mixins import ChannelYoutubeSearchMixin, \
    ChannelYoutubeStatisticsMixin
from segment.models import SegmentChannel
from segment.models import SegmentKeyword
from segment.models import SegmentVideo
# pylint: disable=import-error
from singledb.api.views.base import SingledbApiView
from singledb.connector import SingleDatabaseApiConnector as Connector, \
    SingleDatabaseApiConnectorException
from userprofile.models import Plan, Subscription
from userprofile.models import UserChannel
from utils.api_views_mixins import SegmentFilterMixin
from utils.csv_export import CassandraExportMixin
from utils.permissions import OnlyAdminUserCanCreateUpdateDelete, \
    or_permission_classes, OnlyAdminUserOrSubscriber, user_has_permission


# pylint: enable=import-error


class ChannelListApiView(
        APIView,
        PermissionRequiredMixin,
        CassandraExportMixin,
        ChannelYoutubeSearchMixin,
        SegmentFilterMixin):
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

    def get(self, request):
        """
        Get procedure
        """
        # search procedure
        if request.user.is_staff and any((
                request.query_params.get("youtube_link"),
                request.query_params.get("youtube_keyword"))):
            return self.search_channels()
        # query params validation
        is_query_params_valid, error = self._validate_query_params()
        if not is_query_params_valid:
            return Response({"error": error}, HTTP_400_BAD_REQUEST)
        # init procedures
        empty_response = {
            "max_page": 1,
            "items_count": 0,
            "items": [],
            "current_page": 1,
        }
        # prepare query params
        query_params = deepcopy(request.query_params)
        query_params._mutable = True
        channels_ids = []
        connector = Connector()
        # own channels
        user = request.user
        own_channels = query_params.get("own_channels", "0")
        user_can_see_own_channels = user.has_perm(
            "userprofile.settings_my_yt_channels")
        if own_channels == "1" and user_can_see_own_channels:
            channels_ids = list(
                user.channels.values_list("channel_id", flat=True))
            if not channels_ids:
                return Response(empty_response)
            try:
                ids_hash = connector.store_ids(list(channels_ids))
            except SingleDatabaseApiConnectorException as e:
                    return Response(data={"error": " ".join(e.args)},
                                    status=HTTP_408_REQUEST_TIMEOUT)
            query_params.update(ids_hash=ids_hash)
        channel_segment_id = self.request.query_params.get("channel_segment")
        video_segment_id = self.request.query_params.get("video_segment")
        if any((channel_segment_id, video_segment_id)):
            segment = self._obtain_segment()
            if segment is None:
                return Response(status=HTTP_404_NOT_FOUND)
            if isinstance(segment, SegmentVideo):
                segment_videos_ids = segment.get_related_ids()
                try:
                    ids_hash = connector.store_ids(list(segment_videos_ids))
                except SingleDatabaseApiConnectorException as e:
                    return Response(data={"error": " ".join(e.args)},
                                    status=HTTP_408_REQUEST_TIMEOUT)
                request_params = {
                    "ids_hash": ids_hash,
                    "fields": "channel_id",
                    "size": 10000}
                try:
                    videos_data = connector.get_video_list(request_params)
                except SingleDatabaseApiConnectorException as e:
                    return Response(data={"error": " ".join(e.args)},
                                    status=HTTP_408_REQUEST_TIMEOUT)
                segment_channels_ids = {
                    obj.get("channel_id") for obj in videos_data.get("items")}
                query_params.pop("video_segment")
            else:
                segment_channels_ids = segment.get_related_ids()
                query_params.pop("channel_segment")
            if channels_ids:
                channels_ids = [
                    channel_id
                    for channel_id in channels_ids
                    if channel_id in segment_channels_ids]
            else:
                channels_ids = segment_channels_ids
            if not channels_ids:
                return Response(empty_response)
            try:
                ids_hash = connector.store_ids(list(channels_ids))
            except SingleDatabaseApiConnectorException as e:
                return Response(data={"error": " ".join(e.args)},
                                status=HTTP_408_REQUEST_TIMEOUT)
            query_params.update(ids_hash=ids_hash)
        # adapt the request params
        self.adapt_query_params(query_params)
        # make call
        try:
            response_data = connector.get_channel_list(query_params)
        except SingleDatabaseApiConnectorException as e:
            return Response(
                data={"error": " ".join(e.args)},
                status=HTTP_408_REQUEST_TIMEOUT)
        # adapt the response data
        self.adapt_response_data(response_data, request.user)
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

        # text_search
        text_search = query_params.pop("text_search", [None])[0]
        if text_search:
            query_params.update(text_search__match=text_search)

        # channel_group
        make("term", "channel_group")
        # <--- filters

    @staticmethod
    def adapt_response_data(response_data, user):
        """
        Adapt SDB response format
        """
        user_channels = set(user.channels.values_list(
            "channel_id", flat=True))
        items = response_data.get("items", [])
        for item in items:
            if "channel_id" in item:
                item["id"] = item.get("channel_id", "")
                item["is_owner"] = item["channel_id"] in user_channels
                del item["channel_id"]
            if "country" in item and item["country"] is None:
                item["country"] = ""
            if "history_date" in item and item["history_date"]:
                item["history_date"] = item["history_date"][:10]

            is_own = item.get("is_owner", False)
            if user.has_perm('userprofile.channel_audience') \
                    or is_own:
                pass
            else:
                item['has_audience'] = False
                item["verified"] = False
                item.pop('audience', None)
                item['brand_safety'] = None
                item['safety_chart_data'] = None
                item.pop('traffic_sources', None)

            if not user.is_staff:
                item.pop("cms__title", None)

            if not user.has_perm('userprofile.channel_aw_performance') \
                    and not is_own:
                item.pop('aw_data', None)

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


class OwnChannelPermissions(BasePermission):
    def has_permission(self, request, view):
        return UserChannel.objects \
            .filter(user=request.user, channel_id=view.kwargs.get("pk")) \
            .exists()


class ChannelRetrieveUpdateDeleteApiView(
    SingledbApiView, ChannelYoutubeStatisticsMixin):
    permission_classes = (
        or_permission_classes(
            user_has_permission('userprofile.channel_details'),
            OwnChannelPermissions,
            OnlyAdminUserOrSubscriber),)
    _connector_get = None
    _connector_put = None

    @property
    def connector_put(self):
        """
        Lazy loaded property.
        Purpose: allows patching it in tests
        """
        if self._connector_put is None:
            self._connector_put = Connector().put_channel
        return self._connector_put

    @property
    def connector_get(self):
        """
        Lazy loaded property.
        Purpose: allows patching it in tests
        """
        if self._connector_get is None:
            self._connector_get = Connector().get_channel
        return self._connector_get

    def put(self, *args, **kwargs):
        data = self.request.data
        permitted_groups = ["influencers", "new", "media", "brands"]
        if "channel_group" in data and data[
            "channel_group"] not in permitted_groups:
            return Response(status=HTTP_400_BAD_REQUEST)
        response = super().put(*args, **kwargs)
        ChannelListApiView.adapt_response_data(
            {'items': [response.data]}, self.request.user)
        return response

    def get(self, *args, **kwargs):
        if self.request.user.is_staff and \
                self.request.query_params.get("from_youtube") == "1":
            return self.obtain_youtube_statistics()
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
                average_views = round(
                    sum([v.get("views", 0) for v in videos]) / len(videos))
            for v in videos:
                v["id"] = v.pop("video_id")
                youtube_published_at = v.pop("youtube_published_at", None)
                if youtube_published_at:
                    v['days'] = (now - parser.parse(youtube_published_at)).days
            response.data["performance"] = {
                'average_views': average_views,
                'videos': videos,
            }

        ChannelListApiView.adapt_response_data(
            {'items': [response.data]}, self.request.user)
        return response

    def delete(self, *args, **kwargs):
        pk = kwargs.get('pk')
        UserChannel.objects \
            .filter(channel_id=pk, user=self.request.user) \
            .delete()
        if not UserChannel.objects.filter(channel_id=pk).exists():
            Connector().unauthorize_channel(pk)
        return Response()


class ChannelSetApiView(SingledbApiView):
    permission_classes = (OnlyAdminUserCanCreateUpdateDelete,)
    connector_delete = Connector().delete_channels


class ChannelAuthenticationApiView(APIView):
    permission_classes = tuple()

    def post(self, request, *args, **kwagrs):
        connector = Connector()
        try:
            data = connector.auth_channel(request.data)
        except SingleDatabaseApiConnectorException as e:
            if e.sdb_response is not None:
                return Response(
                    data=e.sdb_response.json(),
                    status=e.sdb_response.status_code
                )
            data = {"error": " ".join(e.args)}
            return Response(data=data, status=HTTP_408_REQUEST_TIMEOUT)

        if data is not None:
            channel_id = data.get('channel_id')
            if channel_id:
                user, created = self.get_or_create_user(
                    data.get("access_token"))
                if not user:
                    return Response(status=HTTP_412_PRECONDITION_FAILED)

                user_channels = user.channels.values_list('channel_id',
                                                          flat=True)
                if channel_id not in user_channels:
                    UserChannel.objects.create(channel_id=channel_id,
                                               user=user)
                    send_new_channel_authentication_email(
                        user, channel_id, request)
                # set user avatar
                if not created:
                    self.set_user_avatar(user, data.get("access_token"))

                return Response(status=HTTP_202_ACCEPTED,
                                data={"auth_token": user.auth_token.key})

        return Response()

    def get_or_create_user(self, access_token):
        """
        After successful channel authentication we create appropriate
         influencer user profile
        In case we've failed to create user - we send None
        :return: user instance or None
        """

        created = False
        # If user is logged in we simply return it
        user = self.request.user
        if user and user.is_authenticated():
            return user, created

        # Starting user create procedure
        token_info_url = "https://www.googleapis.com/oauth2/v3/tokeninfo" \
                         "?access_token={}".format(access_token)
        try:
            response = requests.get(token_info_url)
        except Exception:
            return None, created
        if response.status_code != 200:
            return None, created
        # Have successfully got basic user data
        response = response.json()
        email = response.get("email")
        try:
            user = get_user_model().objects.get(email=email)
        except get_user_model().DoesNotExist:
            google_id = response.get("sub")
            # Obtaining user extra data
            user_data = ChannelAuthenticationApiView.obtain_extra_user_data(
                access_token, google_id)
            # Create new user
            user_data["email"] = email
            user_data["password"] = hashlib.sha1(str(
                timezone.now().timestamp()).encode()).hexdigest()
            user = get_user_model().objects.create(**user_data)
            user.set_password(user.password)
            plan = Plan.objects.get(name=settings.DEFAULT_ACCESS_PLAN_NAME)
            subscription = Subscription.objects.create(user=user, plan=plan)
            user.update_permissions_from_subscription(subscription)
            user.access = settings.DEFAULT_USER_ACCESS
            user.save()
            # Get or create auth token instance for user
            Token.objects.get_or_create(user=user)
            created = True
            send_welcome_email(user, self.request)
            self.check_user_segment_access(user)
        return user, created

    def check_user_segment_access(self, user):
        user_channel_segment = SegmentChannel.objects.filter(shared_with__contains=[user.email]).exists()
        user_video_segment = SegmentVideo.objects.filter(shared_with__contains=[user.email]).exists()
        user_keyword_segment = SegmentKeyword.objects.filter(shared_with__contains=[user.email]).exists()
        if any([user_channel_segment, user_video_segment, user_keyword_segment]):
            user.update_access([{'name': 'Segments', 'value': True}, ])

    @staticmethod
    def obtain_extra_user_data(token, user_id):
        """
        Get user profile extra fields from userinfo
        :param token: oauth2 access token
        :param user_id: google user id
        :return: image link, name
        """
        url = 'https://www.googleapis.com/plus/v1/people/{}/' \
              '?access_token={}'.format(user_id, token)
        try:
            response = requests.get(url)
        except Exception:
            extra_details = {}
        else:
            extra_details = response.json()
        user_data = {
            "first_name": extra_details.get("name", {}).get("givenName", ""),
            "last_name": extra_details.get("name", {}).get("familyName", ""),
            "profile_image_url": None,
            "last_login": timezone.now()
        }
        if not extra_details.get("image", {}).get("isDefault", True):
            user_data["profile_image_url"] = extra_details.get(
                "image", {}).get("url", "").replace("sz=50", "sz=250")
        return user_data

    def set_user_avatar(self, user, access_token):
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
                              "?access_token={}".format(user_google_id,
                                                        access_token)
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
            user.profile_image_url = profile_image_url
            user.save()
        # <-- set user avatar
        return

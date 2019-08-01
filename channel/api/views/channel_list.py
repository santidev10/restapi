import re
from copy import deepcopy
from drf_yasg import openapi
from datetime import datetime
from datetime import timedelta

from rest_framework_csv.renderers import CSVStreamingRenderer
from rest_framework.generics import ListAPIView
from rest_framework.permissions import IsAdminUser

from es_components.connections import init_es_connection
from es_components.constants import Sections
from es_components.managers.channel import ChannelManager

from utils.api.research import ResearchPaginator
from utils.api.research import ESBrandSafetyFilterBackend
from utils.api.research import ESQuerysetResearchAdapter

from utils.api.filters import FreeFieldOrderingFilter
from utils.es_components_api_utils import APIViewMixin
from utils.permissions import or_permission_classes
from utils.permissions import user_has_permission

init_es_connection()

TERMS_FILTER = ("general_data.country", "general_data.top_language", "general_data.top_category",
                "custom_properties.preferred", "analytics.verified", "analytics.cms_title",
                "stats.channel_group", "main.id")

MATCH_PHRASE_FILTER = ("general_data.title",)

RANGE_FILTER = ("social.instagram_followers", "social.twitter_followers", "social.facebook_likes",
                "stats.views_per_video", "stats.engage_rate", "stats.sentiment", "stats.last_30day_views",
                "stats.last_30day_subscribers", "stats.subscribers", "ads_stats.average_cpv", "ads_stats.ctr_v",
                "ads_stats.video_view_rate", "analytics.age13_17", "analytics.age18_24",
                "analytics.age25_34", "analytics.age35_44", "analytics.age45_54",
                "analytics.age55_64", "analytics.age65_")

EXISTS_FILTER = ("general_data.emails", "ads_stats", "analytics")

CHANNEL_ITEM_SCHEMA = openapi.Schema(
    title="Youtube channel",
    type=openapi.TYPE_OBJECT,
    properties=dict(
        description=openapi.Schema(type=openapi.TYPE_STRING),
        id=openapi.Schema(type=openapi.TYPE_STRING),
        subscribers=openapi.Schema(type=openapi.TYPE_STRING),
        thumbnail_image_url=openapi.Schema(type=openapi.TYPE_STRING),
        title=openapi.Schema(type=openapi.TYPE_STRING),
        videos=openapi.Schema(type=openapi.TYPE_STRING),
        views=openapi.Schema(type=openapi.TYPE_STRING),
    ),
)
CHANNELS_SEARCH_RESPONSE_SCHEMA = openapi.Schema(
    title="Youtube channel paginated response",
    type=openapi.TYPE_OBJECT,
    properties=dict(
        max_page=openapi.Schema(type=openapi.TYPE_INTEGER),
        items_count=openapi.Schema(type=openapi.TYPE_INTEGER),
        current_page=openapi.Schema(type=openapi.TYPE_INTEGER),
        items=openapi.Schema(
            title="Youtube channel list",
            type=openapi.TYPE_ARRAY,
            items=CHANNEL_ITEM_SCHEMA,
        ),
    ),
)


class ChannelListCSVRendered(CSVStreamingRenderer):
    header = [
        "title",
        "url",
        "country",
        "category",
        "emails",
        "subscribers",
        "thirty_days_subscribers",
        "thirty_days_views",
        "views_per_video",
        "sentiment",
        "engage_rate",
        "last_video_published_at",
        "brand_safety_score",
        "video_view_rate",
        "ctr",
        "ctr_v",
        "average_cpv"
    ]


class UserChannelsNotAvailable(Exception):
    pass


# todo: refactor/remove it
def adapt_response_channel_data(response_data, user):
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

        for field in ["youtube_published_at", "updated_at"]:
            if field in item and item[field]:
                item[field] = re.sub(
                    "^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+|)$",
                    "\g<0>Z",
                    item[field]
                )
    return response_data


def add_chart_data(channels):
    """ Generate and add chart data for channel """
    for channel in channels:
        if not channel.stats:
            continue

        items = []
        items_count = 0
        history = zip(
            reversed(channel.stats.subscribers_history or []),
            reversed(channel.stats.views_history or [])
        )
        for subscribers, views in history:
            timestamp = channel.stats.historydate - timedelta(
                    days=len(channel.stats.subscribers_history) - items_count - 1)
            timestamp = datetime.combine(timestamp, datetime.max.time())
            items_count += 1
            if any((subscribers, views)):
                items.append(
                    {"created_at": str(timestamp) + "Z",
                     "subscribers": subscribers,
                     "views": views}
                )
        channel.chart_data = items
    return channels


class ChannelListApiView(APIViewMixin, ListAPIView):
    permission_classes = (
        or_permission_classes(
            user_has_permission("userprofile.channel_list"),
            user_has_permission("userprofile.settings_my_yt_channels"),
            IsAdminUser
        ),
    )
    filter_backends = (FreeFieldOrderingFilter, ESBrandSafetyFilterBackend)
    pagination_class = ResearchPaginator
    ordering_fields = (
        "stats.last_30day_subscribers:desc",
        "stats.last_30day_views:desc",
        "stats.subscribers:desc",
        "stats.sentiment:desc",
        "stats.views_per_video:desc",
    )

    terms_filter = TERMS_FILTER
    range_filter = RANGE_FILTER
    match_phrase_filter = MATCH_PHRASE_FILTER
    exists_filter = EXISTS_FILTER

    allowed_aggregations = (
        "ads_stats.average_cpv:max",
        "ads_stats.average_cpv:min",
        "ads_stats.average_cpv:percentiles",
        "ads_stats.ctr_v:max",
        "ads_stats.ctr_v:min",
        "ads_stats.ctr_v:percentiles",
        "ads_stats.video_view_rate:max",
        "ads_stats.video_view_rate:min",
        "ads_stats.video_view_rate:percentiles",
        "ads_stats:exists",
        "analytics.age13_17:max",
        "analytics.age13_17:min",
        "analytics.age18_24:max",
        "analytics.age18_24:min",
        "analytics.age25_34:max",
        "analytics.age25_34:min",
        "analytics.age35_44:max",
        "analytics.age35_44:min",
        "analytics.age45_54:max",
        "analytics.age45_54:min",
        "analytics.age55_64:max",
        "analytics.age55_64:min",
        "analytics.age65_:max",
        "analytics.age65_:min",
        "analytics.cms_title",
        "analytics.gender_female:max",
        "analytics.gender_female:min",
        "analytics.gender_male:max",
        "analytics.gender_male:min",
        "analytics.gender_other:max",
        "analytics.gender_other:min",
        "analytics:exists",
        "analytics:missing",
        "custom_properties.emails:exists",
        "custom_properties.emails:missing",
        "custom_properties.preferred",
        "general_data.country",
        "general_data.top_category",
        "general_data.top_language",
        "social.facebook_likes:max",
        "social.facebook_likes:min",
        "social.facebook_likes:percentiles",
        "social.instagram_followers:max",
        "social.instagram_followers:min",
        "social.instagram_followers:percentiles",
        "social.twitter_followers:max",
        "social.twitter_followers:min",
        "social.twitter_followers:percentiles",
        "stats.last_30day_subscribers:max",
        "stats.last_30day_subscribers:min",
        "stats.last_30day_subscribers:percentiles",
        "stats.last_30day_views:max",
        "stats.last_30day_views:min",
        "stats.last_30day_views:percentiles",
        "stats.subscribers:max",
        "stats.subscribers:min",
        "stats.subscribers:percentiles",
        "stats.views_per_video:max",
        "stats.views_per_video:min",
        "stats.views_per_video:percentiles",
    )

    def get_queryset(self):
        sections = (Sections.MAIN, Sections.GENERAL_DATA, Sections.STATS, Sections.ADS_STATS,
                    Sections.CUSTOM_PROPERTIES, Sections.SOCIAL)
        channels_ids = self.get_own_channel_ids(self.request.user, deepcopy(self.request.query_params))
        if channels_ids:
            self.request.query_params["main.id"] = channels_ids

        if self.request.user.is_staff or channels_ids:
            sections += (Sections.ANALYTICS,)
        return ESQuerysetResearchAdapter(ChannelManager(sections), max_items=100)\
            .extra_fields_func((add_chart_data,))

    @staticmethod
    def get_own_channel_ids(user, query_params):
        own_channels = int(query_params.get("own_channels", "0"))
        user_can_see_own_channels = user.has_perm("userprofile.settings_my_yt_channels")

        if own_channels and not user_can_see_own_channels:
            raise UserChannelsNotAvailable

        if own_channels and user_can_see_own_channels:
            channels_ids = list(user.channels.values_list("channel_id", flat=True))

            if not channels_ids:
                raise UserChannelsNotAvailable

            return channels_ids

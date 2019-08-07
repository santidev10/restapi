import logging

from datetime import datetime
from datetime import timedelta

from keywords.api.utils import get_keywords_aw_stats
from keywords.api.utils import get_keywords_aw_top_bottom_stats
from utils.permissions import OnlyAdminUserCanCreateUpdateDelete

from copy import deepcopy
from rest_framework_csv.renderers import CSVStreamingRenderer
from rest_framework.generics import ListAPIView
from rest_framework.permissions import IsAdminUser

from es_components.constants import Sections
from es_components.managers.keyword import KeywordManager
from es_components.managers.channel import ChannelManager

from utils.api.research import ResearchPaginator
from utils.api.research import ESBrandSafetyFilterBackend

from utils.api.filters import FreeFieldOrderingFilter
from utils.es_components_api_utils import APIViewMixin
from utils.permissions import or_permission_classes
from utils.permissions import user_has_permission
from utils.api.research import ESQuerysetResearchAdapter
from utils.api.research import ESRetrieveApiView
from utils.api.research import ESRetrieveAdapter


TERMS_FILTER = ("stats.is_viral", "stats.top_category",)

MATCH_PHRASE_FILTER = ("main.id",)

RANGE_FILTER = ("stats.search_volume", "stats.average_cpc", "stats.competition",)


logger = logging.getLogger(__name__)



def add_aw_stats(items):
    from aw_reporting.models import BASE_STATS, CALCULATED_STATS, \
        dict_norm_base_stats, dict_add_calculated_stats

    keywords = set(item.main.id for item in items)
    stats = get_keywords_aw_stats(keywords)
    top_bottom_stats = get_keywords_aw_top_bottom_stats(keywords)

    for item in items:
        item_stats = stats.get(item.main.id)
        if item_stats:
            dict_norm_base_stats(item_stats)
            dict_add_calculated_stats(item_stats)
            del item_stats['keyword']
            item.aw_stats = item_stats

            item_top_bottom_stats = top_bottom_stats.get(item.main.id)
            item.aw_stats.update(item_top_bottom_stats)
    return items

def add_views_history_chart(keywords):
    for keyword in keywords:
        items = []
        items_count = 0
        today = datetime.now()
        if keyword.stats and keyword.stats.views_history:
                history = reversed(keyword.stats.views_history)
                for views in history:
                    timestamp = today - timedelta(days=len(keyword.stats.views_history) - items_count - 1)
                    timestamp = datetime.combine(timestamp, datetime.max.time())
                    items_count += 1
                    if views:
                        items.append(
                            {"created_at": timestamp.strftime('%Y-%m-%d'),
                             "views": views}
                        )
        keyword.views_history_chart = items
    return keywords


class KeywordListCSVRendered(CSVStreamingRenderer):
    header = [
        "keyword",
        "search_volume",
        "average_cpc",
        "competition",
        "video_count",
        "views",
    ]


class KeywordListApiView(APIViewMixin, ListAPIView):
    permission_classes = (
        or_permission_classes(
            user_has_permission("userprofile.keyword_list"),
            IsAdminUser
        ),
    )
    filter_backends = (FreeFieldOrderingFilter, ESBrandSafetyFilterBackend)
    pagination_class = ResearchPaginator
    ordering_fields = (
        "stats.last_30day_views:desc",
        "stats.search_volume:desc",
        "stats.average_cpc:desc",
        "stats.competition:desc",
    )

    terms_filter = TERMS_FILTER
    range_filter = RANGE_FILTER
    match_phrase_filter = MATCH_PHRASE_FILTER

    allowed_aggregations = (
        "stats.search_volume:min",
        "stats.search_volume:max",
        "stats.average_cpc:min",
        "stats.average_cpc:max",
        "stats.competition:min",
        "stats.competition:max",
    )

    allowed_percentiles = (
        "stats.search_volume:percentiles",
        "stats.average_cpc:percentiles",
        "stats.competition:percentiles",
    )

    def get_queryset(self):
        sections = (Sections.MAIN, Sections.STATS,)

        query_params = deepcopy(self.request.query_params)
        if query_params.get("from_channel"):
            channel_id = query_params.get("from_channel")
            channel = ChannelManager().model.get(channel_id, _source=(f"{Sections.GENERAL_DATA}.video_tags"))
            keyword_ids = channel.general_data.video_tags

            if keyword_ids:
                self.request.query_params._mutable = True
                self.request.query_params["main.id"] = keyword_ids
                self.terms_filter = self.terms_filter + ("main.id",)

        return ESQuerysetResearchAdapter(KeywordManager(sections), max_items=100).\
            extra_fields_func((add_aw_stats, add_views_history_chart,))


class KeywordRetrieveUpdateApiView(ESRetrieveApiView):
    permission_classes = (
        or_permission_classes(
            user_has_permission("userprofile.keyword_details"),
            OnlyAdminUserCanCreateUpdateDelete
        ),
    )

    def get_object(self):
        keyword = self.kwargs.get("pk")
        logging.info("keyword id {}".format(keyword))
        sections = (Sections.MAIN, Sections.STATS,)

        return ESRetrieveAdapter(KeywordManager(sections))\
            .id(keyword).fields().extra_fields_func((add_aw_stats, add_views_history_chart,))\
            .get_data()

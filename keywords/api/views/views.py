import logging
from copy import deepcopy

from django.contrib.auth.mixins import PermissionRequiredMixin
from rest_framework.response import Response
from rest_framework.status import HTTP_408_REQUEST_TIMEOUT
from rest_framework.views import APIView
from rest_framework_csv.renderers import CSVStreamingRenderer

from keywords.api.utils import get_keywords_aw_stats
from keywords.api.utils import get_keywords_aw_top_bottom_stats
from singledb.api.views import SingledbApiView
from singledb.connector import SingleDatabaseApiConnector as Connector
from singledb.connector import SingleDatabaseApiConnectorException
from singledb.settings import DEFAULT_KEYWORD_DETAILS_FIELDS
from singledb.settings import DEFAULT_KEYWORD_LIST_FIELDS
from utils.permissions import OnlyAdminUserCanCreateUpdateDelete

logger = logging.getLogger(__name__)


class KeywordListCSVRendered(CSVStreamingRenderer):
    header = [
        "keyword",
        "search_volume",
        "average_cpc",
        "competition",
        "video_count",
        "views",
    ]


class KeywordListApiView(APIView, PermissionRequiredMixin):
    """
    Proxy view for keywords list
    """
    permission_classes = tuple()
    permission_required = (
        "userprofile.keyword_list",
    )
    export_file_title = "keyword"
    renderer = KeywordListCSVRendered

    default_request_fields = DEFAULT_KEYWORD_LIST_FIELDS

    def get(self, request):
        connector = Connector()

        # prepare query params
        query_params = deepcopy(request.query_params)
        query_params._mutable = True
        empty_response = {
            "max_page": 1,
            "items_count": 0,
            "items": [],
            "current_page": 1,
        }

        if query_params.get("from_channel"):
            channel = query_params.get("from_channel")
            channel_data = connector.get_channel(query_params=EmptyQueryDict(), pk=channel)
            if channel_data.get('tags'):
                keyword_ids = channel_data.get('tags').split(',')
                ids_hash = connector.store_ids(keyword_ids)
                query_params.update(ids_hash=ids_hash)

        if not request.user.has_perm("userprofile.keyword_list"):
            return Response(empty_response)

        # adapt the request params
        self.adapt_query_params(query_params)

        try:
            response_data = connector.get_keyword_list(query_params)
        except SingleDatabaseApiConnectorException as e:
            return Response(
                data={"error": " ".join(e.args)},
                status=HTTP_408_REQUEST_TIMEOUT)

        # adapt the response data
        self.adapt_response_data(request=self.request, response_data=response_data)
        return Response(response_data)

    @staticmethod
    def adapt_query_params(query_params):
        """
        Adapt SDB request format
        """
        # adapt keyword_text for terms search
        keyword_text = query_params.pop('keyword_text', [None])[0]
        if keyword_text:
            query_params.update(**{"keyword_text": keyword_text.replace(' ', ',')})

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

        # min_search_volume, max_search_volume
        make_range("search_volume")

        # min_average_cpc, max_average_cpc
        make_range("average_cpc")

        # min_competition, max_competition
        make_range("competition")

        # keyword
        make("terms", "keyword")

        # keyword_text search
        make("terms", "keyword_text")

        # viral
        is_viral = query_params.pop("is_viral", [None])[0]
        if is_viral is not None:
            query_params.update(
                is_viral__term="false" if is_viral == "0" else "true")

        # category
        make("terms", "category")

        # <--- filters

    @staticmethod
    def adapt_response_data(request, response_data):
        """
        Adapt SDB response format
        """
        items = response_data.get("items", [])
        from aw_reporting.models import BASE_STATS, CALCULATED_STATS, \
            dict_norm_base_stats, dict_add_calculated_stats

        keywords = set(i['keyword'] for i in items)
        stats = get_keywords_aw_stats(keywords)
        top_bottom_stats = get_keywords_aw_top_bottom_stats(keywords)

        aw_fields = BASE_STATS + tuple(CALCULATED_STATS.keys()) + ("campaigns_count",)
        for item in items:
            item_stats = stats.get(item['keyword'])
            if item_stats:
                dict_norm_base_stats(item_stats)
                dict_add_calculated_stats(item_stats)
                del item_stats['keyword']
                item.update(item_stats)

                item_top_bottom_stats = top_bottom_stats.get(item['keyword'])
                item.update(item_top_bottom_stats)
            else:
                item.update({f: 0 if f == "campaigns_count" else None for f in aw_fields})


class KeywordRetrieveUpdateApiView(SingledbApiView):
    permission_classes = (OnlyAdminUserCanCreateUpdateDelete,)
    permission_required = ('userprofile.keyword_details',)
    _connector_get = None
    default_request_fields = DEFAULT_KEYWORD_DETAILS_FIELDS

    @property
    def connector_get(self):
        if self._connector_get is None:
            self._connector_get = Connector().get_keyword
        return self._connector_get

    def get(self, *args, **kwargs):
        response = super().get(*args, **kwargs)
        if not response.data.get('error'):
            KeywordListApiView.adapt_response_data(request=self.request, response_data={'items': [response.data]})
        return response


class EmptyQueryDict(dict):
    pass
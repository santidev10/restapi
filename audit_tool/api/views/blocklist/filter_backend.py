from rest_framework.filters import BaseFilterBackend

from es_components.managers import VideoManager
from es_components.managers import ChannelManager
from es_components.models import Channel
from es_components.query_builder import QueryBuilder
from utils.es_components_api_utils import ESQuerysetAdapter


class BlocklistESFilterBackend(BaseFilterBackend):
    def _get_fields(self, request):
        fields = self._get_fields(request)
        return fields

    def _get_query_params(self, request):
        params = request.query_params.dict()
        params["custom_properties.blocklist"] = True
        return params

    def filter_queryset(self, request, queryset, view):
        search_term = request.query_params.get("search")
        if search_term:
            es_manager = ChannelManager if len(queryset) > 0 and isinstance(queryset[0], Channel) \
                else VideoManager
            queryset = ESQuerysetAdapter(es_manager(), from_cache=False)
            query = self._get_search_filter(request, search_term)
            result = queryset.filter([query])
            return result
        return queryset

    def _get_search_filter(self, request, search_term):
        if "youtube.com" in search_term:
            separator = "/channel/" if request.parser_context["kwargs"]["data_type"] == "channel" else "?v="
            search_term = search_term.split(separator)[-1]
        query = (QueryBuilder().build().must().term().field("main.id").value(search_term).get()
            | QueryBuilder().build().must().match_phrase().field("general_data.title").value(search_term).get()) \
            & QueryBuilder().build().must().term().field("custom_properties.blocklist").value(True).get()
        return query

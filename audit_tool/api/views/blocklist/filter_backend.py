from elasticsearch_dsl import Q
from rest_framework.filters import BaseFilterBackend

from es_components.constants import Sections
from es_components.query_builder import QueryBuilder


class BlocklistESFilterBackend(BaseFilterBackend):
    def _get_fields(self, request):
        fields = self._get_fields(request)
        return fields

    def _get_query_params(self, request):
        params = request.query_params.dict()
        params["custom_properties.blocklist"] = True
        return params

    def filter_queryset(self, request, queryset, view):
        query = QueryBuilder().build().must().term().field(f"{Sections.CUSTOM_PROPERTIES}.blocklist").value(True).get() \
             & self._get_search_filter(request)
        result = queryset.filter([query])
        return result

    def _get_search_filter(self, request):
        search_term = request.query_params.get("search")
        if search_term:
            if "youtube.com" in search_term:
                separator = "/channel/" if request.parser_context["kwargs"]["data_type"] == "channel" else "?v="
                search_term = search_term.split(separator)[-1]
            query = QueryBuilder().build().must().term().field("main.id").value(search_term).get() \
                | QueryBuilder().build().must().match_phrase().field("general_data.title").value(search_term).get()
        else:
            query = Q("bool")
        return query

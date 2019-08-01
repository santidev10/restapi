import logging

from rest_framework.filters import BaseFilterBackend
from rest_framework.serializers import BaseSerializer

from es_components.query_builder import QueryBuilder
from utils.api.filters import FreeFieldOrderingFilter
from utils.api_paginator import CustomPageNumberPaginator

DEFAULT_PAGE_SIZE = 50

logger = logging.getLogger(__name__)


def get_limits(query_params, default_page_size=None, max_page_number=None):
    size = int(query_params.get("size", default_page_size or DEFAULT_PAGE_SIZE))
    page = int(query_params.get("page", 1))
    page = min(page, max_page_number) if max_page_number else page
    offset = 0 if page <= 1 else (page - 1) * size

    return size, offset, page


def get_sort_rule(query_params):
    sort_params = query_params.get("sort", None)

    if sort_params:
        key, direction = sort_params.split(":")
        return [{key: {"order": direction}}]


def get_fields(query_params, allowed_sections_to_load):
    fields = query_params.get("fields", [])

    if fields:
        fields = fields.split(",")

    fields = [
        field
        for field in fields
        if field.split(".")[0] in allowed_sections_to_load
    ]

    return fields


class QueryGenerator:
    es_manager = None
    terms_filter = ()
    range_filter = ()
    match_phrase_filter = ()
    exists_filter = ()

    def __init__(self, query_params):
        self.query_params = query_params

    def __get_filter_range(self):
        filters = []

        for field in self.range_filter:

            range = self.query_params.get(field, None)

            if range:
                min, max = range.split(",")

                if not (min or max):
                    continue

                query = QueryBuilder().build().must().range().field(field)
                if min:
                    query = query.gte(int(min))
                if max:
                    query = query.lte(int(max))
                filters.append(query.get())

        return filters

    def __get_filters_term(self):
        filters = []

        for field in self.terms_filter:

            value = self.query_params.get(field, None)
            if value:
                value = value.split(",")
                filters.append(
                    QueryBuilder().build().must().terms().field(field).value(value).get()
                )

        return filters

    def __get_filters_match_phrase(self):
        filters = []

        for field in self.match_phrase_filter:
            value = self.query_params.get(field, None)
            if value:
                filters.append(
                    QueryBuilder().build().must().match_phrase().field(field).value(value).get()
                )

        return filters

    def __get_filters_exists(self):
        filters = []

        for field in self.exists_filter:
            value = self.query_params.get(field, None)

            if value is None:
                continue

            query = QueryBuilder().build()

            if value is True or value == "true":
                query = query.must()
            else:
                query = query.must_not()
            filters.append(query.exists().field(field).get())

        return filters

    def get_search_filters(self, ids=None):
        filters_term = self.__get_filters_term()
        filters_range = self.__get_filter_range()
        filters_match_phrase = self.__get_filters_match_phrase()
        filters_exists = self.__get_filters_exists()
        forced_filter = [self.es_manager.forced_filters()]

        filters = filters_term + filters_range + filters_match_phrase + \
                  filters_exists + forced_filter

        if ids:
            filters.append(self.es_manager.ids_query(ids))

        return filters


class ESSerializer(BaseSerializer):
    def to_representation(self, instance):
        return instance.to_dict()


class ESQuerysetAdapter:
    def __init__(self, manager, max_items=None):
        self.manager = manager
        self.sort = None
        self.filter_query = None
        self.max_items = max_items
        self.slice = None
        self.aggregations = None
        self.fields_to_load = None

    def count(self):
        count = self.manager.search(filters=self.filter_query).count()
        return min(count, self.max_items or count)

    def order_by(self, *sorting):
        key, direction = sorting[0].split(":")
        self.sort = [
            {key: {"order": direction}},
            {"main.id": {"order": "asc"}}
        ]
        return self

    def filter(self, query):
        self.filter_query = query
        return self

    def fields(self, fields):
        fields = [
            field
            for field in fields
            if field.split(".")[0] in self.manager.sections
        ]

        self.fields_to_load = fields or self.manager.sections
        return self

    def get_data(self, start=0, end=None):
        return self.manager.search(
            filters=self.filter_query,
            sort=self.sort,
            offset=start,
            limit=end,
        ) \
            .source(includes=self.fields_to_load).execute().hits

    def get_aggregations(self):
        return self.manager.get_aggregation(
            search=self.manager.search(filters=self.filter_query),
            properties=self.aggregations,
        )

    def with_aggregations(self, aggregations):
        self.aggregations = aggregations
        return self

    def __getitem__(self, item):
        if isinstance(item, slice):
            return self.get_data(item.start, item.stop)
        if isinstance(item, int):
            return self.get_data(end=item)
        raise NotImplementedError


class ESFilterBackend(BaseFilterBackend):
    def _get_query_generator(self, request, queryset, view):
        dynamic_generator_class = type(
            "DynamicGenerator",
            (QueryGenerator,),
            dict(
                es_manager=queryset.manager,
                terms_filter=view.terms_filter,
                range_filter=view.range_filter,
                match_phrase_filter=view.match_phrase_filter,
                exists_filter=view.exists_filter,
            )
        )
        return dynamic_generator_class(request.query_params)

    def _get_aggregations(self, request, queryset, view):
        aggregations = request.query_params.get("aggregations", "").split(",")
        if view.allowed_aggregations is not None:
            aggregations = [
                agg for agg in aggregations
                if agg in view.allowed_aggregations
            ]
        return aggregations

    def _get_fields(self, request):
        fields = request.query_params.get("fields", "").split(",")
        return fields

    def filter_queryset(self, request, queryset, view):
        if not isinstance(queryset, ESQuerysetAdapter):
            raise BrokenPipeError
        query_generator = self._get_query_generator(request, queryset, view)
        query = query_generator.get_search_filters()
        fields = self._get_fields(request)
        aggregations = self._get_aggregations(request, queryset, view)
        return queryset.filter(query).fields(fields).with_aggregations(aggregations)


class APIViewMixin:
    serializer_class = ESSerializer
    filter_backends = (FreeFieldOrderingFilter, ESFilterBackend)

    allowed_aggregations = ()

    terms_filter = ()
    range_filter = ()
    match_phrase_filter = ()
    exists_filter = ()


class PaginatorWithAggregationMixin:
    def _get_response_data(self: CustomPageNumberPaginator, data):
        response_data = super(PaginatorWithAggregationMixin, self)._get_response_data(data)
        object_list = self.page.paginator.object_list
        if isinstance(object_list, ESQuerysetAdapter):
            response_data["aggregations"] = object_list.get_aggregations() or None
        else:
            logger.warning("Can't get aggregation from %s", str(type(object_list)))
        return response_data

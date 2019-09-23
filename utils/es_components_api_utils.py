import hashlib
import json
import logging
from urllib.parse import unquote

from django.core.serializers.json import DjangoJSONEncoder
from rest_framework.filters import BaseFilterBackend
from rest_framework.serializers import Serializer

from es_components.query_builder import QueryBuilder
from utils.api.filters import FreeFieldOrderingFilter
from utils.api_paginator import CustomPageNumberPaginator
from utils.es_components_cache import CACHE_KEY_PREFIX
from utils.es_components_cache import cached_method
from utils.percentiles import get_percentiles

DEFAULT_PAGE_SIZE = 50
UI_STATS_HISTORY_FIELD_LIMIT = 30

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

    def add_should_filters(self, ranges, filters, field):
        if ranges is None:
            return
        queries = []
        for range in ranges:
            if range:
                min, max = range.split(",")

                if not (min or max):
                    continue

                query = QueryBuilder().build().should().range().field(field)
                if min:
                    try:
                        min = float(min)
                    except ValueError:
                        # in case of filtering by date
                        pass
                    query = query.gte(min)
                if max:
                    try:
                        max = float(max)
                    except ValueError:
                        # in case of filtering by date
                        pass
                    query = query.lte(max)
                queries.append(query)
        combined_query = None
        for query in queries:
            query_obj = query.get()
            if not combined_query:
                combined_query = query_obj
            else:
                combined_query |= query_obj
        filters.append(combined_query)

    def __get_filter_range(self):
        filters = []

        for field in self.range_filter:
            if field == "brand_safety.overall_score":
                self.add_should_filters(self.query_params.get(field, None), filters, field)
            else:
                range = self.query_params.get(field, None)
                if range:
                    min, max = range.split(",")

                    if not (min or max):
                        continue

                    query = QueryBuilder().build().must().range().field(field)
                    if min:
                        try:
                            min = float(min)
                        except ValueError:
                            # in case of filtering by date
                            pass
                        query = query.gte(min)
                    if max:
                        try:
                            max = float(max)
                        except ValueError:
                            # in case of filtering by date
                            pass
                        query = query.lte(max)
                    filters.append(query.get())

        return filters

    def __get_filters_term(self):
        filters = []

        for field in self.terms_filter:

            value = self.query_params.get(field, None)
            if value:
                value = value.split(",") if isinstance(value, str) else value
                filters.append(
                    QueryBuilder().build().must().terms().field(field).value(value).get()
                )

        return filters

    def __get_filters_match_phrase(self):
        filters = []

        for field in self.match_phrase_filter:
            value = self.query_params.get(field, None)
            if value and isinstance(value, str):
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
            elif value is False or value == "false":
                query = query.must_not()
            else:
                continue
            filters.append(query.exists().field(field).get())

        return filters

    def __get_filters_by_ids(self):
        """
        DEPRECATED
        Use __get_filters_term with "main.id" term
        """
        ids_str = self.query_params.get("ids", None)
        filters = []
        if ids_str:
            ids = ids_str.split(",")
            filters.append(self.es_manager.ids_query(ids))
        return filters

    def get_search_filters(self):
        filters_term = self.__get_filters_term()
        filters_range = self.__get_filter_range()
        filters_match_phrase = self.__get_filters_match_phrase()
        filters_exists = self.__get_filters_exists()
        forced_filter = [self.es_manager.forced_filters()]
        ids_filter = self.__get_filters_by_ids()

        filters = filters_term + filters_range + filters_match_phrase + \
                  filters_exists + forced_filter + ids_filter

        return filters


class ESDictSerializer(Serializer):
    def to_representation(self, instance):
        extra_data = super(ESDictSerializer, self).to_representation(instance)

        chart_data = extra_data.get("chart_data")
        if chart_data and isinstance(chart_data, list):
            chart_data[:] = chart_data[-UI_STATS_HISTORY_FIELD_LIMIT:]
        data = instance.to_dict()
        stats = data.get("stats", {})
        for name, value in stats.items():
            if name.endswith("_history") and isinstance(value, list):
                value[:] = value[:UI_STATS_HISTORY_FIELD_LIMIT]
        return {
            **data,
            **extra_data,
        }


class ESQuerysetAdapter:
    def __init__(self, manager, *args, **kwargs):
        self.manager = manager
        self.sort = None
        self.filter_query = None
        self.slice = None
        self.aggregations = None
        self.percentiles = None
        self.fields_to_load = None
        self.search_limit = None

    @cached_method(timeout=7200)
    def count(self):
        count = self.manager.search(filters=self.filter_query).count()
        return count

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

    def with_limit(self, search_limit):
        self.search_limit = search_limit
        return self

    @cached_method(timeout=900)
    def get_data(self, start=0, end=None):
        data = self.manager.search(
            filters=self.filter_query,
            sort=self.sort,
            offset=start,
            limit=end,
        ) \
            .source(includes=self.fields_to_load).execute().hits
        return data

    @cached_method(timeout=7200)
    def get_aggregations(self):
        aggregations = self.manager.get_aggregation(
            search=self.manager.search(filters=self.filter_query),
            properties=self.aggregations,
        )
        return aggregations

    def get_percentiles(self):
        clean_names = [name.split(":")[0] for name in self.percentiles]
        percentiles = get_percentiles(self.manager, clean_names, add_suffix=":percentiles")
        return percentiles

    def with_aggregations(self, aggregations):
        self.aggregations = aggregations
        return self

    def with_percentiles(self, percentiles):
        self.percentiles = percentiles
        return self

    def get_cache_key(self, part, options):
        options = dict(
            filters=[_filter.to_dict() for _filter in self.filter_query],
            sort=self.sort,
            aggregations=self.aggregations,
            options=options,
        )
        key_json = json.dumps(options, sort_keys=True, cls=DjangoJSONEncoder)
        key_hash = hashlib.md5(key_json.encode()).hexdigest()
        key = f"{CACHE_KEY_PREFIX}.{part}.{self.manager.model.__name__}.{key_hash}"
        return key, key_json

    def __getitem__(self, item):
        if isinstance(item, slice):
            return self.get_data(item.start, item.stop)
        raise NotImplementedError

    def __iter__(self):
        if self.sort or self.search_limit:
            yield from self.get_data(end=self.search_limit)
        else:
            yield from self.manager.scan(
                filters=self.filter_query,
            )


class ESFilterBackend(BaseFilterBackend):
    def _get_query_params(self, request):
        return request.query_params.dict()

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
        query_params = self._get_query_params(request)
        return dynamic_generator_class(query_params)

    def _get_aggregations(self, request, queryset, view):
        query_params = self._get_query_params(request)
        aggregations = unquote(query_params.get("aggregations", "")).split(",")
        if "flags" in aggregations:
            flags_index = aggregations.index("flags")
            aggregations[flags_index] = "stats.flags"
        if view.allowed_aggregations is not None:
            aggregations = [agg
                            for agg in aggregations
                            if agg in view.allowed_aggregations]
        return aggregations

    def _get_percentiles(self, request, queryset, view):
        query_params = self._get_query_params(request)
        percentiles = unquote(query_params.get("aggregations", "")).split(",")
        if view.allowed_percentiles is not None:
            percentiles = [agg
                           for agg in percentiles
                           if agg in view.allowed_percentiles]
        return percentiles

    def _get_fields(self, request):
        query_params = self._get_query_params(request)
        fields = query_params.get("fields", "").split(",")
        return fields

    def filter_queryset(self, request, queryset, view):
        from utils.api.research import ESEmptyResponseAdapter

        if isinstance(queryset, ESEmptyResponseAdapter):
            return []
        if not isinstance(queryset, ESQuerysetAdapter):
            raise BrokenPipeError
        query_generator = self._get_query_generator(request, queryset, view)
        query = query_generator.get_search_filters()
        fields = self._get_fields(request)
        aggregations = self._get_aggregations(request, queryset, view)
        percentiles = self._get_percentiles(request, queryset, view)
        result = queryset.filter(query) \
            .fields(fields) \
            .with_aggregations(aggregations) \
            .with_percentiles(percentiles)
        return result


class ESPOSTFilterBackend(ESFilterBackend):
    def _get_query_params(self, request):
        query_params = super()._get_query_params(request)
        if request.method == "POST":
            query_params.update(request.data)
        return query_params


class APIViewMixin:
    serializer_class = ESDictSerializer
    filter_backends = (FreeFieldOrderingFilter, ESFilterBackend)

    allowed_aggregations = ()
    allowed_percentiles = ()

    terms_filter = ()
    range_filter = ()
    match_phrase_filter = ()
    exists_filter = ()


class PaginatorWithAggregationMixin:
    def _get_response_data(self: CustomPageNumberPaginator, data):
        response_data = super(PaginatorWithAggregationMixin, self)._get_response_data(data)
        object_list = self.page.paginator.object_list
        if isinstance(object_list, ESQuerysetAdapter):
            aggregations = object_list.get_aggregations() or {}
            percentiles = object_list.get_percentiles() or {}
            all_aggregations = dict(**aggregations, **percentiles)
            response_data["aggregations"] = all_aggregations or None
        else:
            logger.warning("Can't get aggregation from %s", str(type(object_list)))
        return response_data



class ExportDataGenerator:
    serializer_class = ESDictSerializer
    terms_filter = ()
    range_filter = ()
    match_phrase_filter = ()
    exists_filter = ()
    queryset = None

    def __init__(self, query_params):
        self.query_params = query_params

    def _get_query_generator(self):
        dynamic_generator_class = type(
            "DynamicGenerator",
            (QueryGenerator,),
            dict(
                es_manager=self.queryset.manager,
                terms_filter=self.terms_filter,
                range_filter=self.range_filter,
                match_phrase_filter=self.match_phrase_filter,
                exists_filter=self.exists_filter,
            )
        )
        return dynamic_generator_class(self.query_params)

    def __iter__(self):
        self.queryset.filter(
            self._get_query_generator().get_search_filters()
        )
        for item in self.queryset:
            yield self.serializer_class(item).data

from rest_framework.filters import BaseFilterBackend
from rest_framework.serializers import BaseSerializer

from es_components.query_builder import QueryBuilder

DEFAULT_PAGE_SIZE = 50


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

                if not (min and max):
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

    def __getitem__(self, item):
        if isinstance(item, slice):
            return self.manager.search(
                filters=self.filter_query,
                sort=self.sort,
                offset=item.start,
                limit=item.stop
            ) \
                .execute().hits
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

    def filter_queryset(self, request, queryset, view):
        if not isinstance(view.queryset, ESQuerysetAdapter):
            raise BrokenPipeError
        query_generator = self._get_query_generator(request, queryset, view)
        query = query_generator.get_search_filters()
        return queryset.filter(query)


class APIViewMixin:
    terms_filter = ()
    range_filter = ()
    match_phrase_filter = ()
    exists_filter = ()

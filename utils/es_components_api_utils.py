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

        filters = filters_term + filters_range + filters_match_phrase +\
            filters_exists + forced_filter

        if ids:
            filters.append(self.es_manager.ids_query(ids))

        return filters

from operator import attrgetter

from elasticsearch_dsl import Q

from es_components.query_builder import QueryBuilder


def bulk_search(model, query, sort, cursor_field, batch_size=10000, min_cursor=1000, max_cursor=None, source=None, options=None):
    """
    Util function to retrieve items greater than Elasticsearch limit by using cursors
    :param model: Elasticsearch model
    :param query: Base query
    :param sort: list
    :param cursor_field: str -> Field to use as cursor when retrieving items
    :param options: list -> Additional queries to sequentially apply to base query
        This is to ensure retrieving items with specific filters in order
        options = [
            QueryBuilder().build().must().term().field(f"{Sections.MONETIZATION}.is_monetizable").value(True).get(),
            QueryBuilder().build().must_not().term().field(f"{Sections.MONETIZATION}.is_monetizable").value(True).get(),
        ]
    :param min_cursor: (int, str) -> Min value for cursor range
    :param max_cursor: (int, str) -> Max value for cursor range
    :param batch_size: int
    :param source: list[str] -> Returned document fields to deserialize
    :return:
    """
    base_search = model.search().sort(*sort)
    if source:
        base_search = base_search.source(source)
    # If no options set, use base query
    if options is None:
        options = [None]
    # Create generator for each option to yield all results from
    generators = [
        search_generator(base_search, query, cursor_field, min_cursor, size=batch_size, max_cursor=max_cursor, option=option)
        for option in options
    ]
    # Yield all results from each generator sequentially
    for gen in generators:
        yield from gen


def search_generator(search, query, cursor_field, min_cursor, size=1000, max_cursor=None, option=None):
    """
    Helper function to encapsulate cursor query
        Search object should have all options except query applied
        Uses min_cursor and max_cursor to query for a range of items on cursor_field
        Max cursor does not change
    :param search: Elasticsearch Search object
    :param query: QueryBuilder object
    :param cursor_field: str
    :param min_cursor: int
    :param size: int
    :param max_cursor: int
    :param option: QueryBuilder object
    :return:
    """
    while True:
        if type(query) is dict:
            query = Q(query)
        # If max cursor, query for range in between min_cursor and max_cursor
        if max_cursor:
            cursor_query = QueryBuilder().build().must().range().field(cursor_field).lt(max_cursor).gte(min_cursor).get()
        # Else query for all items >= min_cursor
        else:
            cursor_query = QueryBuilder().build().must().range().field(cursor_field).gte(min_cursor).get()
        full_query = query & cursor_query & option if option is not None else query & cursor_query
        search.query = full_query
        results = search[0:size].execute()
        data = results.hits
        if data:
            yield data
            min_cursor = attrgetter(cursor_field)(data[-1])
        else:
            break

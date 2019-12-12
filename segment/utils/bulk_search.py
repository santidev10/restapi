from operator import attrgetter

from es_components.query_builder import QueryBuilder


def bulk_search(model, query, sort, cursor_field, options=None, max_cursor=None, min_cursor=1000, batch_size=10000, source=None):
    """
    Util function to retrieve items greater than Elasticsearch limit by using cursors
    :param model: Elasticsearch model
    :param query: Base query
    :param sort: list
    :param cursor_field: str -> Field to use as cursor when retrieving items
    :param options: list -> Additional queries to sequentially apply to base query
        options = [
            QueryBuilder().build().must().term().field(f"{Sections.MONETIZATION}.is_monetizable").value(True).get(),
            QueryBuilder().build().must_not().term().field(f"{Sections.MONETIZATION}.is_monetizable").value(True).get(),
        ]
    :param max_cursor: int -> Max value for cursor range
    :param min_cursor: int -> Min value for cursor range
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
        search_generator(base_search, query, max_cursor, cursor_field, min_cursor, batch_size, option)
        for option in options
    ]
    # Yield all results from each generator sequentially
    for gen in generators:
        yield from gen


def search_generator(search, query, max_cursor, cursor_field, min_cursor, size, option=None):
    """
    Helper function to encapsulate cursor query
        Search object have all options except query applied
    :param search: Elasticsearch Search object
    :param query: QueryBuilder object
    :param max_cursor: int
    :param cursor_field: str
    :param min_cursor: int
    :param size: int
    :param option: QueryBuilder object
    :return:
    """
    while True:
        if max_cursor:
            cursor_query = QueryBuilder().build().must().range().field(cursor_field).lt(max_cursor).gte(min_cursor).get()
        else:
            cursor_query = QueryBuilder().build().must().range().field(cursor_field).gte(min_cursor).get()
        full_query = query & cursor_query & option if option is not None else query & cursor_query
        search.query = full_query
        results = search[0:size].execute().hits
        if results:
            yield results
            max_cursor = attrgetter(cursor_field)(results[-1])
        else:
            break

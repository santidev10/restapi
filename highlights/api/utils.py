from utils.api_paginator import CustomPageNumberPaginator
from utils.es_components_api_utils import PaginatorWithAggregationMixin


class HighlightsPaginator(PaginatorWithAggregationMixin, CustomPageNumberPaginator):
    page_size = 20
    page_size_query_param = "size"
    max_page_number = 5

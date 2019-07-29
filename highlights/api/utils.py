from utils.api_paginator import CustomPageNumberPaginator


class HighlightsPaginator(CustomPageNumberPaginator):
    page_size = 20
    page_size_query_param = "size"
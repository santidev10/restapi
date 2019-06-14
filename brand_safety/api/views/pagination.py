from utils.api_paginator import CustomPageNumberPaginator


class BrandSafetyPaginator(CustomPageNumberPaginator):
    page_size = 50
    page_size_query_param = "size"

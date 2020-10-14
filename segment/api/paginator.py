from utils.api_paginator import CustomPageNumberPaginator


class SegmentPaginator(CustomPageNumberPaginator):
    """
    Paginator for segments list
    """
    page_size = 10
    page_size_query_param = "page_size"

    def get_page_size(self, request):
        page_size = super().get_page_size(request)
        size_query = request.query_params.get("size")
        if size_query and size_query.isnumeric():
            page_size = int(size_query)
        return page_size

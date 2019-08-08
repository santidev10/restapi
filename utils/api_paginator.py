"""
Custom api paginator module
"""
from django.core.paginator import InvalidPage
from django.core.paginator import Page
from django.core.paginator import Paginator
from django.utils.functional import cached_property
from rest_framework.pagination import _positive_int
from rest_framework.response import Response
from rest_framework.settings import api_settings


class CustomPageNumberPaginator:
    """
    Customize page number paginator response
    """
    page_query_param = "page"
    page_size_query_param = "size"
    max_page_size = None
    page_size = api_settings.PAGE_SIZE
    max_page_number = None

    def __init__(self):
        self.page = None
        self.request = None

    def get_paginated_response(self, data):
        """
        Update response to return
        """
        return Response(self._get_response_data(data))

    def _get_response_data(self, data):
        return {
            'items_count': self.page.paginator.count,
            'items': data,
            'current_page': self.page.number,
            'max_page': self.page.paginator.num_pages,
        }

    def get_page_size(self, request):
        if self.page_size_query_param:
            try:
                return _positive_int(
                    request.query_params[self.page_size_query_param],
                    strict=False,
                    cutoff=self.max_page_size
                )
            except (KeyError, ValueError):
                pass

        return self.page_size

    def paginate_queryset(self, queryset, request, view=None):
        page_size = self.get_page_size(request)

        paginator = PaginatorWithZeroPage(queryset, page_size, max_page_number=self.max_page_number)
        page_number = request.query_params.get(self.page_query_param, 1)

        try:
            self.page = paginator.page(page_number)
        except InvalidPage:
            self.page = Page([], 0, paginator)

        return list(self.page)


class PaginatorWithZeroPage(Paginator):
    def __init__(self, *args, max_page_number=None, **kwargs):
        super(PaginatorWithZeroPage, self).__init__(*args, **kwargs)
        self.max_page_number = max_page_number

    @cached_property
    def num_pages(self):
        if self.per_page == 0:
            return 0
        num_pages = super(PaginatorWithZeroPage, self).num_pages
        return min(num_pages, self.max_page_number or num_pages)

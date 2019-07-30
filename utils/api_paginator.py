"""
Custom api paginator module
"""
from rest_framework.pagination import PageNumberPagination
from rest_framework.response import Response


class CustomPageNumberPaginator(PageNumberPagination):
    """
    Customize page number paginator response
    """

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

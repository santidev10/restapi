from rest_framework.pagination import PageNumberPagination
from rest_framework.response import Response


class ListPaginator(PageNumberPagination):
    page_size = 20

    def get_paginated_response(self, data):
        response_data = {
            'items_count': self.page.paginator.count,
            'items': data,
            'current_page': self.page.number,
            'max_page': self.page.paginator.num_pages,
        }
        return Response(response_data)

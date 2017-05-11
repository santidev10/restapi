from django.http import HttpResponse
from django.http import StreamingHttpResponse
from django.utils import timezone
from rest_framework.pagination import PageNumberPagination
from rest_framework.response import Response
from rest_framework.generics import ListCreateAPIView, RetrieveAPIView, UpdateAPIView
from rest_framework.status import HTTP_404_NOT_FOUND, HTTP_400_BAD_REQUEST, \
    HTTP_200_OK, HTTP_202_ACCEPTED, HTTP_403_FORBIDDEN, HTTP_405_METHOD_NOT_ALLOWED
from rest_framework.views import APIView
from aw_creation.api.serializers import *
from aw_reporting.demo import demo_view_decorator
from datetime import datetime
from io import StringIO
import logging
import csv
import re

logger = logging.getLogger(__name__)


class OptimizationAccountListPaginator(PageNumberPagination):
    page_size = 100

    def get_paginated_response(self, data):
        """
        Update response to return
        """
        response_data = {
            'items_count': self.page.paginator.count,
            'items': data,
            'current_page': self.page.number,
            'max_page': self.page.paginator.num_pages,
        }
        return Response(response_data)


class OptimizationAccountListApiView(ListCreateAPIView):

    serializer_class = OptimizationAccountListSerializer
    pagination_class = OptimizationAccountListPaginator

    def get_queryset(self):
        queryset = AccountCreation.objects.filter(
            owner=self.request.user
        ).order_by('is_ended', '-created_at')

        return queryset

    def filter_queryset(self, queryset):
        now = timezone.now()
        if not self.request.query_params.get('show_closed', False):
            queryset = queryset.filter(
                Q(campaign_managements__end__isnull=True) |
                Q(campaign_managements__end__gte=now)
            )
        search = self.request.query_params.get('search', '').strip()
        if search:
            queryset = queryset.filter(name__icontains=search)
        return queryset.distinct()

    def post(self, request, *args, **kwargs):
        self.user_ids_to_emails(request.data)
        response = super(AccountManagementListApiView,
                         self).post(request, *args, **kwargs)
        return response


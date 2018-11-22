from django.contrib.auth import get_user_model
from django.db.models import Value, Q
from django.db.models.functions import Concat
from rest_framework.generics import ListAPIView
from rest_framework.permissions import IsAdminUser
from rest_framework.response import Response
from rest_framework.status import HTTP_400_BAD_REQUEST

from administration.api.serializers import UserSerializer
from utils.api_paginator import CustomPageNumberPaginator


class UserPaginator(CustomPageNumberPaginator):
    """
    Paginator for user list
    """
    page_size = 10


class UserListAdminApiView(ListAPIView):
    """
    Admin user list endpoint
    """
    serializer_class = UserSerializer
    pagination_class = UserPaginator
    permission_classes = (IsAdminUser,)
    allowed_sorts = (
        "last_name",
        "first_name",
        "status",
        "date_joined"
    )

    def is_query_params_valid(self):
        order_by = self.request.query_params.get("order_by")
        if order_by and order_by not in self.allowed_sorts:
            return Response(
                {"query_param_value_invalid": "{} can be one of: {}".format(
                    "order_by", ", ".join(self.allowed_sorts))},
                status=HTTP_400_BAD_REQUEST)
        return True

    def get(self, request, *args, **kwargs):
        is_valid_query_params = self.is_query_params_valid()
        if isinstance(is_valid_query_params, Response):
            return is_valid_query_params
        return super(UserListAdminApiView, self).get(request, *args, **kwargs)

    def get_queryset(self):
        """
        Exclude requested user from queryset
        """
        queryset = get_user_model().objects.exclude(id=self.request.user.id)
        queryset = self.do_filters(queryset)
        queryset = self.do_sorts(queryset)
        return queryset

    def do_filters(self, queryset):
        search = self.request.query_params.get("search")
        if search:
            search = search.strip()
            queryset = queryset.annotate(full_name=Concat("first_name", Value(" "), "last_name"))
            queryset = queryset.filter(
                Q(full_name__icontains=search) |
                Q(email__icontains=search) |
                Q(company__icontains=search) |
                Q(phone_number__icontains=search)
            ).distinct()
        status = self.request.query_params.get("status")
        if status:
            queryset = queryset.filter(status=status)
        return queryset

    def do_sorts(self, queryset):
        sort_by = self.request.query_params.get("sort_by")
        if not sort_by:
            return queryset.order_by("last_name", "first_name")
        return queryset.order_by(sort_by)


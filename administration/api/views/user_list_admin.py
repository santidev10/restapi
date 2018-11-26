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


class UserListAdminQueryParamsNames:
    SORT_BY = "sort_by"
    ASCENDING = "ascending"
    SEARCH = "search"
    STATUS = "status"


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
    allowed_ascending_value = "1"

    def validate_query_params(self):
        sort_by = self.request.query_params.get(UserListAdminQueryParamsNames.SORT_BY)
        if sort_by and sort_by not in self.allowed_sorts:
            raise Exception(
                "{} can be one of: {}".format(UserListAdminQueryParamsNames.SORT_BY, ", ".join(self.allowed_sorts)))
        ascending = self.request.query_params.get(UserListAdminQueryParamsNames.ASCENDING)
        if ascending and ascending != self.allowed_ascending_value:
            raise Exception("{} can have only {} value".format(
                UserListAdminQueryParamsNames.ASCENDING, self.allowed_ascending_value))

    def get(self, request, *args, **kwargs):
        try:
            self.validate_query_params()
        except Exception as e:
            return Response(data={"query_param_value_invalid": str(e)}, status=HTTP_400_BAD_REQUEST)
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
        search = self.request.query_params.get(UserListAdminQueryParamsNames.SEARCH)
        if search:
            search = search.strip()
            queryset = queryset.annotate(full_name=Concat("first_name", Value(" "), "last_name"))
            queryset = queryset.filter(
                Q(full_name__icontains=search) |
                Q(email__icontains=search) |
                Q(company__icontains=search) |
                Q(phone_number__icontains=search)
            ).distinct()
        status = self.request.query_params.get(UserListAdminQueryParamsNames.STATUS)
        if status:
            queryset = queryset.filter(status=status)
        return queryset

    def do_sorts(self, queryset):
        sort_by = self.request.query_params.get(UserListAdminQueryParamsNames.SORT_BY)
        if not sort_by:
            return queryset.order_by("pk")
        ascending = self.request.query_params.get(UserListAdminQueryParamsNames.ASCENDING)
        sort_prefix = ""
        if ascending is None:
            sort_prefix = "-"
        return queryset.order_by("{}{}".format(sort_prefix, sort_by))

import operator
from functools import reduce

from django.core.exceptions import ValidationError
from django.db.models import Q
from rest_framework import permissions
from rest_framework.generics import ListCreateAPIView
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.status import HTTP_201_CREATED

from administration.api.serializers import UserActionCreateSerializer
from administration.api.serializers import UserActionRetrieveSerializer
from administration.models import UserAction
from userprofile.api.views.user_finalize_response import UserFinalizeResponse
from userprofile.constants import StaticPermissions
from utils.api_paginator import CustomPageNumberPaginator


class UserActionPermission(permissions.BasePermission):
    """
    require admin access for GET requests (to see who's taken what actions)
    and authenticated access for POST requests (to tell us what actions they've taken)
    """

    def has_permission(self, request, view):
        if request.method == "GET":
            return request.user and request.user.has_permission(StaticPermissions.ADMIN)
        return IsAuthenticated.has_permission(self, request, view)


class UserActionPaginator(CustomPageNumberPaginator):
    """
    Paginator for user action list
    """
    page_size = 20


class UserActionListCreateApiView(UserFinalizeResponse, ListCreateAPIView):
    """
    User action list / create endpoint
    """
    pagination_class = UserActionPaginator
    serializer_class = UserActionRetrieveSerializer
    create_serializer_class = UserActionCreateSerializer
    permission_classes = (UserActionPermission,)

    def post(self, request, *args, **kwargs):
        """
        Add current user to post data
        """
        # add user id to data
        data = request.data.copy()
        data["user"] = request.user.id
        # serialization procedure
        serializer_class = self.create_serializer_class
        kwargs["context"] = self.get_serializer_context()
        serializer = serializer_class(data=data, *args, **kwargs)
        serializer.is_valid(raise_exception=True)
        instance = serializer.save()
        headers = self.get_success_headers(serializer.data)
        return Response(
            self.serializer_class(instance).data,
            status=HTTP_201_CREATED, headers=headers)

    def do_filters(self, queryset):
        """
        Apply filters for queryset
        """
        # search by user
        search = self.request.query_params.get("username")
        if search:
            search = search.strip()
            search = search.split(" ")
            first_name_search = reduce(
                operator.or_, (
                    Q(user__first_name__icontains=x) for x in search))
            last_name_serach = reduce(
                operator.or_, (
                    Q(user__last_name__icontains=x) for x in search))
            email_search = reduce(
                operator.or_, (
                    Q(user__email__icontains=x) for x in search))
            queryset = queryset.filter(
                first_name_search | last_name_serach | email_search)
        filters = {}
        # url
        url = self.request.query_params.get("url")
        if url:
            filters["url__icontains"] = url
        # slug
        slug = self.request.query_params.get("slug")
        if slug:
            filters["slug__icontains"] = slug
        # start date
        start_date = self.request.query_params.get("start_date")
        if start_date:
            filters["created_at__gte"] = start_date
        # end date
        end_date = self.request.query_params.get("end_date")
        if end_date:
            filters["created_at__lte"] = end_date
        if filters:
            try:
                queryset = queryset.filter(**filters)
            except ValidationError:
                queryset = UserAction.objects.none()
        return queryset

    def do_sorts(self, queryset):
        """
        Apply sorts for queryset
        """
        order_by = self.request.query_params.get("order_by")
        if not order_by:
            return queryset
        available_sorts = {
            "slug",
            "url"
        }
        available_reverse_sorts = {
            "created_at"
        }
        if order_by in available_sorts:
            return queryset.order_by("{}".format(order_by))
        if order_by in available_reverse_sorts:
            return queryset.order_by("-{}".format(order_by))
        return queryset

    def paginate_queryset(self, queryset):
        """
        Processing flat query param
        """
        flat = self.request.query_params.get("flat")
        if flat == "1":
            return None
        return super().paginate_queryset(queryset)

    def get_queryset(self):
        """
        Prepare queryset
        """
        queryset = UserAction.objects.all() \
            .prefetch_related("user")
        return self.do_sorts(self.do_filters(queryset))

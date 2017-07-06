"""
Administration api views module
"""
import operator

from django.contrib.auth import get_user_model
from functools import reduce

from django.db.models import Q
from rest_framework.views import APIView
from rest_framework.generics import ListAPIView, DestroyAPIView, \
    ListCreateAPIView
from rest_framework.permissions import IsAdminUser
from rest_framework.response import Response
from rest_framework.status import HTTP_404_NOT_FOUND, HTTP_403_FORBIDDEN, \
    HTTP_201_CREATED
from rest_framework.authtoken.models import Token

from administration.api.serializers import UserActionRetrieveSerializer, \
    UserActionCreateSerializer
from administration.models import UserAction
from userprofile.models import UserProfile
from userprofile.api.serializers import UserSerializer
from utils.api_paginator import CustomPageNumberPaginator


class UserPaginator(CustomPageNumberPaginator):
    """
    Paginator for user list
    """
    page_size = 10


class UserActionPaginator(CustomPageNumberPaginator):
    """
    Paginator for user action list
    """
    page_size = 20


class UserListAdminApiView(ListAPIView):
    """
    Admin user list endpoint
    """
    serializer_class = UserSerializer
    pagination_class = UserPaginator
    permission_classes = (IsAdminUser, )

    def get_queryset(self):
        """
        Exclude requested user from queryset
        """
        return get_user_model().objects.exclude(id=self.request.user.id)


class UserDeleteAdminApiView(DestroyAPIView):
    """
    Admin user delete endpoint
    """
    permission_classes = (IsAdminUser, )

    def get_queryset(self):
        """
        Exclude requested user from queryset
        """
        return get_user_model().objects.exclude(id=self.request.user.id)


class AuthAsAUserAdminApiView(APIView):
    """
    Login as a user
    """
    permission_classes = (IsAdminUser, )

    def get(self, request, pk, **_):
        """
        Get the selected user and return its data
        :param request:
        :param pk: uid of the user
        :param args:
        :param kwargs:
        :return: user's data(including the token key)
        """
        try:
            user = UserProfile.objects.get(pk=pk)
        except UserProfile.DoesNotExist:
            return Response(status=HTTP_404_NOT_FOUND)
        Token.objects.get_or_create(user=user)
        response_data = UserSerializer(user).data
        return Response(response_data)


class UserActionListCreateApiView(ListCreateAPIView):
    """
    User action list / create endpoint
    """
    pagination_class = UserActionPaginator
    serializer_class = UserActionRetrieveSerializer
    create_serializer_class = UserActionCreateSerializer

    def post(self, request, *args, **kwargs):
        """
        Add current user to post data
        """
        # add user id to data
        request.data["user"] = request.user.id
        # serialization procedure
        serializer_class = self.create_serializer_class
        kwargs['context'] = self.get_serializer_context()
        serializer = serializer_class(data=request.data, *args, **kwargs)
        serializer.is_valid(raise_exception=True)
        instance = serializer.save()
        headers = self.get_success_headers(serializer.data)
        return Response(
            self.serializer_class(instance).data,
            status=HTTP_201_CREATED, headers=headers)

    def get(self, request, *args, **kwargs):
        """
        Check admin permission
        """
        if not request.user.is_staff:
            return Response(status=HTTP_403_FORBIDDEN)
        return super(UserActionListCreateApiView, self).get(
            request, *args, **kwargs)

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
        if filters:
            queryset = queryset.filter(**filters)
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
        elif order_by in available_reverse_sorts:
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
        queryset = UserAction.objects.all()
        return self.do_sorts(self.do_filters(queryset))


class UserActionDeleteAdminApiView(DestroyAPIView):
    """
    User action delete endpoint
    """
    permission_classes = (IsAdminUser, )
    queryset = UserAction.objects.all()

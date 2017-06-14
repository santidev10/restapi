"""
Administration api views module
"""
from django.contrib.auth import get_user_model
from rest_framework.views import APIView
from rest_framework.generics import ListAPIView, DestroyAPIView
from rest_framework.permissions import IsAdminUser
from rest_framework.response import Response
from rest_framework.status import HTTP_404_NOT_FOUND
from rest_framework.authtoken.models import Token

from userprofile.models import UserProfile
from userprofile.api.serializers import UserSerializer
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

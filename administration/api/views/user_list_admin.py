from django.contrib.auth import get_user_model
from django.db.models import Value, Q
from django.db.models.functions import Concat
from rest_framework.generics import ListAPIView
from rest_framework.permissions import IsAdminUser

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
            queryset = queryset.annotate(full_name=Concat('first_name', Value(' '), 'last_name'))
            queryset = queryset.filter(
                Q(full_name__icontains=search) |
                Q(email__icontains=search) |
                Q(company__icontains=search) |
                Q(phone_number__icontains=search)
            ).distinct()
        return queryset

    def do_sorts(self, queryset):
        return queryset.order_by('last_name', 'first_name')

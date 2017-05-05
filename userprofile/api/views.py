"""
Userprofile api views module
"""
from django.contrib.auth import get_user_model
from rest_framework.generics import CreateAPIView
from rest_framework.response import Response
from rest_framework.status import HTTP_201_CREATED

from userprofile.api.serializers import UserCreateSerializer, UserSerializer


class UserCreateApiView(CreateAPIView):
    """
    User list / create endpoint
    """
    permission_classes = tuple()
    serializer_class = UserCreateSerializer
    retrieve_serializer_class = UserSerializer
    queryset = get_user_model()

    def post(self, request, *args, **kwargs):
        """
        Extend post functionality
        """
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        user = serializer.save()
        response_data = UserSerializer(user).data
        return Response(response_data, status=HTTP_201_CREATED)

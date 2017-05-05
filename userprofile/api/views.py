"""
Userprofile api views module
"""
from django.contrib.auth import get_user_model
from rest_framework.authtoken.models import Token
from rest_framework.authtoken.serializers import AuthTokenSerializer
from rest_framework.response import Response
from rest_framework.status import HTTP_201_CREATED, HTTP_401_UNAUTHORIZED
from rest_framework.views import APIView

from userprofile.api.serializers import UserCreateSerializer, UserSerializer


class UserCreateApiView(APIView):
    """
    User list / create endpoint
    """
    permission_classes = tuple()
    serializer_class = UserCreateSerializer
    retrieve_serializer_class = UserSerializer
    queryset = get_user_model()

    def post(self, request):
        """
        Extend post functionality
        """
        serializer = self.serializer_class(data=request.data)
        serializer.is_valid(raise_exception=True)
        user = serializer.save()
        response_data = self.retrieve_serializer_class(user).data
        return Response(response_data, status=HTTP_201_CREATED)


class UserAuthApiView(APIView):
    """
    Login / logout endpoint
    """
    permission_classes = tuple()
    serializer_class = UserSerializer

    def post(self, request):
        """
        Login user
        """
        serializer = AuthTokenSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        user = serializer.validated_data['user']
        Token.objects.get_or_create(user=user)
        response_data = self.serializer_class(user).data
        return Response(response_data)

    def delete(self, request):
        """
        Logout user
        """
        if not request.user.is_authenticated():
            return Response(status=HTTP_401_UNAUTHORIZED)
        Token.objects.get(user=request.user).delete()
        return Response()


class UserProfileApiView(APIView):
    """
    User profile api view
    """
    serializer_class = UserSerializer

    def get(self, request):
        """
        Retrieve profile
        """
        response_data = self.serializer_class(request.user).data
        return Response(response_data)

    def put(self, request):
        """
        Update profile
        """
        serializer = self.serializer_class(
            instance=request.user, data=request.data, partial=True)
        serializer.is_valid(raise_exception=True)
        serializer.save()
        return Response(serializer.data)

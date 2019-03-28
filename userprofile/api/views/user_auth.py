import requests
from django.conf import settings
from django.contrib.auth import get_user_model
from django.contrib.auth.models import update_last_login
from drf_yasg import openapi
from drf_yasg.utils import swagger_auto_schema
from rest_framework.authtoken.models import Token
from rest_framework.response import Response
from rest_framework.status import HTTP_400_BAD_REQUEST
from rest_framework.status import HTTP_401_UNAUTHORIZED
from rest_framework.views import APIView
from drf_yasg.utils import swagger_auto_schema
from userprofile.api.serializers import UserSerializer
from userprofile.utils import is_apex_user
from userprofile.utils import is_correct_apex_domain
from drf_yasg import openapi

LOGIN_REQUEST_SCHEMA = openapi.Schema(
    title="Login request",
    type=openapi.TYPE_OBJECT,
    properties=dict(
        username=openapi.Schema(type=openapi.TYPE_STRING),
        password=openapi.Schema(type=openapi.TYPE_STRING),
    ),
)

LOGIN_REQUEST_SCHEMA = openapi.Schema(
    title="Login request",
    type=openapi.TYPE_OBJECT,
    properties=dict(
        username=openapi.Schema(type=openapi.TYPE_STRING),
        password=openapi.Schema(type=openapi.TYPE_STRING),
    ),
)


class UserAuthApiView(APIView):
    """
    Login / logout endpoint
    """
    permission_classes = tuple()
    serializer_class = UserSerializer

    @swagger_auto_schema(request_body=LOGIN_REQUEST_SCHEMA)
    def post(self, request):
        """
        Login user
        """
        token = request.data.get("token")
        auth_token = request.data.get("auth_token")
        update_date_of_last_login = True
        if token:
            user = self.get_google_user(token)
        elif auth_token:
            try:
                user = Token.objects.get(key=auth_token).user
            except Token.DoesNotExist:
                user = None
            else:
                update_date_of_last_login = False
        else:
            user_email = request.data.get("username")
            try:
                user = get_user_model().objects.get(email=user_email)
            except get_user_model().DoesNotExist:
                user = None
            else:
                user_password = request.data.get("password")
                if not user.check_password(user_password):
                    user = None

        if not user:
            return Response(
                data={
                    "error": ["Unable to authenticate user"
                              " with provided credentials"]
                },
                status=HTTP_400_BAD_REQUEST)

        request_origin = request.META.get("HTTP_ORIGIN") or request.META.get("HTTP_REFERER")

        if is_apex_user(user.email) and not is_correct_apex_domain(request_origin):
            return Response(
                data={
                    "error": ["Unable to authenticate APEX user"
                              " on this site. Please go to {}".format(settings.APEX_HOST)]
                },
                status=HTTP_400_BAD_REQUEST)

        Token.objects.get_or_create(user=user)
        if update_date_of_last_login:
            update_last_login(None, user)

        response_data = self.serializer_class(user).data

        custom_auth_flags = settings.CUSTOM_AUTH_FLAGS.get(user.email.lower())
        if custom_auth_flags:
            for name, value in custom_auth_flags.items():
                response_data[name] = value

        return Response(response_data)

    def delete(self, request):
        """
        Logout user
        """
        if not request.user.is_authenticated():
            return Response(status=HTTP_401_UNAUTHORIZED)
        Token.objects.get(user=request.user).delete()
        return Response()

    def get_google_user(self, token):
        """
        Check token is valid and grab google user
        """
        url = "https://www.googleapis.com/oauth2/v3/tokeninfo" \
              "?access_token={}".format(token)
        try:
            response = requests.get(url)
        except Exception as e:
            return None
        if response.status_code != 200:
            return None
        response = response.json()
        aud = response.get("aud")
        if aud != settings.GOOGLE_APP_AUD:
            return None

        email = response.get("email")
        try:
            user = get_user_model().objects.get(email__iexact=email)
        except get_user_model().DoesNotExist:
            return None
        return user

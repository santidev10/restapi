import requests
from django.conf import settings
from django.contrib.auth import get_user_model
from django.contrib.auth.models import update_last_login
from rest_framework.authtoken.models import Token
from rest_framework.authtoken.serializers import AuthTokenSerializer
from rest_framework.response import Response
from rest_framework.status import HTTP_400_BAD_REQUEST
from rest_framework.status import HTTP_401_UNAUTHORIZED
from rest_framework.views import APIView

from userprofile.api.serializers import UserSerializer


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
        token = request.data.get("token")
        auth_token = request.data.get("auth_token")
        update_date_of_last_login = True
        if token:
            user = self.get_google_plus_user(token)
        elif auth_token:
            try:
                user = Token.objects.get(key=auth_token).user
            except Token.DoesNotExist:
                user = None
            else:
                update_date_of_last_login = False
        else:
            serializer = AuthTokenSerializer(data=request.data)
            serializer.is_valid(raise_exception=True)
            user = serializer.validated_data['user']
        if not user:
            return Response(
                data={
                    "error": ["Unable to authenticate user"
                              " with provided credentials"]
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

    def get_google_plus_user(self, token):
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

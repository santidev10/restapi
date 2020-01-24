from botocore.exceptions import ClientError
import boto3
from datetime import timedelta

import requests
from django.conf import settings
from django.contrib.auth import get_user_model
from django.contrib.auth.models import update_last_login
from django.utils import timezone
from drf_yasg import openapi
from drf_yasg.utils import swagger_auto_schema
from rest_framework.authtoken.models import Token
from rest_framework.exceptions import ValidationError
from rest_framework.response import Response
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_400_BAD_REQUEST
from rest_framework.status import HTTP_401_UNAUTHORIZED
from rest_framework.views import APIView

from userprofile.api.serializers import UserSerializer
from userprofile.utils import is_apex_user
from userprofile.utils import is_correct_apex_domain

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
        Each handler method returns a Response object that is used to return from the view
            Each handler also may also raise ValidationErrors if necessary
        """
        data = request.data
        token = data.get("token")
        auth_token = data.get("auth_token", "")
        answer = data.get("answer")
        username = data.get("username")
        password = data.get("password")
        session = data.get("session")
        mfa_type = data.get("mfa_type")
        is_temp_auth = auth_token.startswith("temp")

        # If auth_token, check if valid token before any operations
        if auth_token:
            user, token = self._validate_get_user_token(auth_token)
            client = boto3.client("cognito-idp") if is_temp_auth else None
            # handle user auth with full access auth_token
            if not is_temp_auth:
                response = self.handle_post_login(user, False)

            # handle create auth challenge with temp auth_token and mfa type
            elif is_temp_auth and mfa_type and not username and not password:
                response = self.mfa_create_challenge(client, user, data)

            # handle submitting / verifying mfa challenge
            elif auth_token and answer and session and is_temp_auth:
                response = self.mfa_submit_challenge(client, user, data)

            else:
                response = self._get_invalid_response()

        # Handle login with username / password and return temp auth_token for mfa
        elif username and password:
            response = self.handle_login(username, password)

        # Google token
        elif token and not auth_token:
            user = self.get_google_user(token)
            response = self.handle_post_login(user, True)

        # Login data sent by client invalid
        else:
            response = self._get_invalid_response()

        return response

    def delete(self, request):
        """
        Logout user
        """
        if not request.user.is_authenticated:
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

    def mfa_create_challenge(self, client, user, data):
        """
        Begin MFA login process by sending user challenge code through preferred medium (text | email)
        """
        mfa_type = data.get("mfa_type", "")
        if mfa_type != "email" and mfa_type != "text":
            raise LoginException("mfa_type option must either be email or text.")
        try:
            # Must set attribute on user since we are unable to send options during define mfa challenge trigger
            client.admin_update_user_attributes(
                UserPoolId=settings.COGNITO_USER_POOL_ID,
                Username=user.email,
                UserAttributes=[
                    {"Name": "phone_number", "Value": user.phone_number},
                    {"Name": "custom:mfa_type", "Value": mfa_type}
                ]
            )
            response = client.admin_initiate_auth(
                UserPoolId=settings.COGNITO_USER_POOL_ID,
                ClientId=settings.COGNITO_CLIENT_ID,
                AuthFlow="CUSTOM_AUTH",
                AuthParameters={
                    "USERNAME": user.email,
                },
            )
        except Exception as e:
            res = str(e)
            status_code = HTTP_400_BAD_REQUEST
        else:
            res = {
                "session": response["Session"],
                "retries": response["ChallengeParameters"]["retries"]
            }
            status_code = HTTP_200_OK
        return Response(data=res, status=status_code)

    def mfa_submit_challenge(self, client, user, data):
        """
        Submit mfa code to Cognito and parse results
            If successful, grant client auth_token to use for subsequent requests
            If incorrect, respond with session for retries
        :param client: Boto3 object
        :param user: UserProfile
        :param data: dict
        :return:
        """
        self.mfa_validate_data(data)
        try:
            result = client.admin_respond_to_auth_challenge(
                UserPoolId=settings.COGNITO_USER_POOL_ID,
                ClientId=settings.COGNITO_CLIENT_ID,
                Session=data["session"],
                ChallengeName="CUSTOM_CHALLENGE",
                ChallengeResponses={
                    "USERNAME": user.email,
                    "ANSWER": data["answer"]
                },
            )
        except ClientError as e:
            Token.objects.filter(user=user).delete()
            message = e.response["Error"]["Message"]
            if not message == "Invalid session for the user.":
                message = "Max retries exceeded."
            message += " Please log in again."
            raise LoginException(message)
        else:
            if result.get("AuthenticationResult"):
                # Delete temp auth_token for mfa process
                Token.objects.filter(user=user).delete()
                Token.objects.create(user=user)
                response = self.handle_post_login(user, True)
            else:
                res = {
                    "session": result["Session"],
                    "retries": result["ChallengeParameters"]["retries"]
                }
                response = Response(data=res, status=HTTP_400_BAD_REQUEST)
            return response

    def mfa_validate_data(self, data):
        errors = []
        try:
            data["session"]
        except KeyError:
            errors.append("Session required for MFA.")
        try:
            data["answer"] = str(data["answer"]).replace("-", "")
        except KeyError:
            errors.append("MFA challenge answer required for MFA.")
        if errors:
            raise LoginException(", ".join(errors))

    def handle_login(self, email, password):
        """
        Validate user login with email and password
        Once validated, send OK response and wait for client to initiate mfa auth by sending
            auth_token prepended with "temp_" and preferred MFA type (text | email)
        :param email: str
        :param password: str
        :return:
        """
        try:
            user = get_user_model().objects.get(email=email)
            if not user.check_password(password):
                raise ValueError
        except get_user_model().DoesNotExist:
            raise LoginException(f"User with email does not exist: {email}.")
        except ValueError:
            raise LoginException("Invalid password.")

        Token.objects.filter(user=user).delete()
        # send back mfa options
        response = {
            "username": email
        }
        if user.phone_number:
            formatted = f"**(***)***-{user.phone_number[-4:]}"
            response["phone_number"] = formatted

        token = Token()
        token.key = f"temp_{token.generate_key()}"[:40]
        token.user = user
        token.save()
        response["auth_token"] = token.key
        return Response(data=response)

    def handle_post_login(self, user, update_date_of_last_login):
        """
        Post login validation
        :param user:
        :param update_date_of_last_login: bool
        :return:
        """
        request_origin = self.request.META.get("HTTP_ORIGIN") or self.request.META.get("HTTP_REFERER")
        if is_apex_user(user.email) and not is_correct_apex_domain(request_origin):
            return Response(
                data={
                    "error": "Unable to authenticate APEX user"
                             " on this site. Please go to <a href='{apex_host}'>"
                             "{apex_host}</a>".format(apex_host=settings.APEX_HOST)
                },
                status=HTTP_400_BAD_REQUEST)

        if update_date_of_last_login:
            update_last_login(None, user)

        response_data = self.serializer_class(user).data

        custom_auth_flags = settings.CUSTOM_AUTH_FLAGS.get(user.email.lower())
        if custom_auth_flags:
            for name, value in custom_auth_flags.items():
                response_data[name] = value
        return Response(response_data)

    def _validate_get_user_token(self, key):
        """
        Retrieve user and token with provided auth_token key
        Also validates if token is valid if was created within expire threshold
        :param key: str
        :return:
        """
        try:
            token = Token.objects.get(key=key)
            threshold = timezone.now() - timedelta(days=settings.AUTH_TOKEN_EXPIRES)
            if token.created < threshold:
                raise ValueError
        except (Token.DoesNotExist, ValueError):
            raise LoginException(f"Invalid token. Please log in again.")
        return token.user, token

    def _get_invalid_response(self):
        response = Response(data="Unable to authenticate user. Please try logging in again.",
                            status=HTTP_400_BAD_REQUEST)
        return response


class LoginException(ValidationError):
    def __init__(self, message):
        data = {
            "message": message,
        }
        super().__init__(data)


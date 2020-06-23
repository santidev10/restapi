from datetime import timedelta

import boto3
import requests
from botocore.exceptions import ClientError
from botocore.exceptions import ParamValidationError
from django.conf import settings
from django.contrib.auth import get_user_model
from django.contrib.auth.models import update_last_login
from django.utils import timezone
from drf_yasg import openapi
from drf_yasg.utils import swagger_auto_schema
from rest_framework.exceptions import ValidationError
from rest_framework.response import Response
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_400_BAD_REQUEST
from rest_framework.status import HTTP_401_UNAUTHORIZED
from rest_framework.views import APIView

from userprofile.api.serializers import UserSerializer
from userprofile.models import UserDeviceToken
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
            and each handler may also raise LoginException ValidationErrors
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
            device_token = self._validate_user_device_token(auth_token)
            user = device_token.user
            client = boto3.client("cognito-idp") if is_temp_auth else None
            # handle user auth with full access auth_token
            if not is_temp_auth:
                response = self.handle_post_login(user, False, device_token)

            # handle create auth challenge with temp auth_token and mfa type
            elif is_temp_auth and mfa_type and not username and not password:
                response = self.mfa_create_challenge(client, user, data)

            # handle submitting / verifying mfa challenge
            elif auth_token and answer and session and is_temp_auth:
                response = self.mfa_submit_challenge(client, user, data, device_token)

            else:
                response = self._get_invalid_response()

        # Handle login with username / password and return temp auth_token for mfa
        elif username and password:
            response = self.handle_login(username, password)

        # Google token
        elif token and not auth_token:
            user = self.get_google_user(token)
            device_auth_token = self.create_device_auth_token(user)
            response = self.handle_post_login(user, True, device_auth_token)

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
        try:
            request.auth.delete()
        except AttributeError:
            pass
        return Response()

    def get_google_user(self, token: str):
        """
        Check token is valid and grab google user
        """
        url = "https://www.googleapis.com/oauth2/v3/tokeninfo" \
              "?access_token={}".format(token)
        try:
            response = requests.get(url)
        # pylint: disable=broad-except
        except Exception:
            return None
        # pylint: enable=broad-except
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
        Create or update user with mfa type and send session

        :param client: boto3
        :param user: UserProfile
        :param data: dict
        :return: Response
        """
        mfa_type = data.get("mfa_type", "")
        if mfa_type not in ("email", "text"):
            raise LoginException("mfa_type option must either be email or text.")

        user_attributes = [{"Name": "custom:mfa_type", "Value": mfa_type}]
        if user.phone_number and user.phone_number_verified:
            user_attributes.append({"Name": "phone_number", "Value": user.phone_number})
        elif mfa_type == "text":
            raise LoginException("You must have a verified phone number to use text MFA.")

        error = None
        should_update = False
        try:
            # Create / Update user with attributes
            client.admin_create_user(
                UserPoolId=settings.COGNITO_USER_POOL_ID,
                Username=user.email,
                UserAttributes=user_attributes,
                MessageAction="SUPPRESS"
            )
        except ParamValidationError as err:
            error = err.kwargs["report"]
        except ClientError as err:
            # User exists, update attributes
            if err.response["Error"]["Code"] == "UsernameExistsException":
                should_update = True
            else:
                error = err.response["Error"]["Message"]

        if should_update:
            try:
                client.admin_update_user_attributes(
                    UserPoolId=settings.COGNITO_USER_POOL_ID,
                    Username=user.email,
                    UserAttributes=user_attributes
                )
            except ClientError as err:
                error = err.response["Error"]["Message"]

        if error:
            raise LoginException(error)
        try:
            response = client.admin_initiate_auth(
                UserPoolId=settings.COGNITO_USER_POOL_ID,
                ClientId=settings.COGNITO_CLIENT_ID,
                AuthFlow="CUSTOM_AUTH",
                AuthParameters={
                    "USERNAME": user.email,
                },
            )
        except ClientError as error:
            raise LoginException(error.response["Error"]["Message"])
        else:
            res = {
                "session": response["Session"],
                "retries": response["ChallengeParameters"]["retries"]
            }
            status_code = HTTP_200_OK
        return Response(data=res, status=status_code)

    def mfa_submit_challenge(self, client, user, data, device_auth_token):
        """
        Submit mfa code to Cognito and parse results
        If successful, grant client auth_token to use for subsequent requests
        If incorrect, respond with session for retries

        :param client: boto3
        :param user: UserProfile
        :param data: dict
        :param device_auth_token: UserDeviceToken
        :return: Response
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
        except ParamValidationError as err:
            raise LoginException(err.kwargs["report"])
        except ClientError as err:
            UserDeviceToken.objects.filter(user=user, key=data["auth_token"]).delete()
            # Most errs will have code NotAuthorizedMessage
            if err.response["Error"]["Message"] != "Incorrect username or password.":
                # Strip message since not all messages returned end with a period
                message = err.response["Error"]["Message"].strip(".")
            else:
                message = "Max attempts exceeded"
            message += ". Please log in again."
            raise LoginException(message)
        else:
            if result.get("AuthenticationResult"):
                # Update temp key for mfa process
                device_auth_token.update_key()
                response = self.handle_post_login(user, True, device_auth_token)
            else:
                res = {
                    "session": result["Session"],
                    "retries": result["ChallengeParameters"]["retries"]
                }
                response = Response(data=res, status=HTTP_400_BAD_REQUEST)
            return response

    def mfa_validate_data(self, data):
        """
        Validate request body for mfa

        :param data: dict
        :return: None
        """
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
        Once validated, send 200 response and wait for client to initiate mfa auth by sending
            auth_token prefixed with "temp_" and preferred MFA type (text | email)

        :param email: str
        :param password: str
        :return: Response
        """
        try:
            user = get_user_model().objects.get(email=email)
            if not user.check_password(password):
                raise ValueError
        except (get_user_model().DoesNotExist, ValueError):
            raise LoginException("That username / password is not valid.")

        # send back mfa options
        response = {
            "username": email
        }
        if user.phone_number and user.phone_number_verified:
            formatted = f"**(***)***-{user.phone_number[-4:]}"
            response["phone_number"] = formatted

        device_token = self.create_device_auth_token(user, is_temp=True)
        response["auth_token"] = device_token.key
        return Response(data=response)

    def handle_post_login(self, user, update_date_of_last_login, device_auth_token=None):
        """
        Post login validation

        :param user: UserProfile
        :param update_date_of_last_login: bool
        :param device_auth_token: UserDeviceToken
        :return: Response
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
        if device_auth_token:
            response_data.update({
                "token": device_auth_token.key,
                "device_id": device_auth_token.device_id
            })

        custom_auth_flags = settings.CUSTOM_AUTH_FLAGS.get(user.email.lower())
        if custom_auth_flags:
            for name, value in custom_auth_flags.items():
                response_data[name] = value
        return Response(response_data)

    def _validate_user_device_token(self, key):
        """
        Retrieve user and device token with provided auth_token key
        Also validates if token is valid if was created within expire threshold

        :param key: str
        :return: UserDeviceToken
        """
        try:
            device_token = UserDeviceToken.objects.get(key=key)
            threshold = timezone.now() - timedelta(days=settings.AUTH_TOKEN_EXPIRES)
            if device_token.created_at < threshold:
                raise ValueError
        except (UserDeviceToken.DoesNotExist, ValueError):
            raise LoginException("Invalid token. Please log in again.")
        return device_token

    def _get_invalid_response(self):
        """
        :return: Response
        """
        response = Response(data="Unable to authenticate user. Please try logging in again.",
                            status=HTTP_400_BAD_REQUEST)
        return response

    @staticmethod
    def create_device_auth_token(user, is_temp=False):
        """
        Set auth_token and reset created timestamp

        :return: UserDeviceToken
        """
        key = UserDeviceToken.generate_key(is_temp=is_temp)
        device_token = UserDeviceToken.objects.create(user=user, key=key)
        return device_token


class LoginException(ValidationError):
    def __init__(self, message):
        data = {
            "message": message,
        }
        super().__init__(data)

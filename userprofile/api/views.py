"""
Userprofile api views module
"""
from itertools import chain

import requests
from django.conf import settings
from django.contrib.auth import get_user_model
from django.contrib.auth.models import update_last_login
from django.contrib.auth.tokens import PasswordResetTokenGenerator
from django.core.mail import send_mail
from rest_framework.authtoken.models import Token
from rest_framework.authtoken.serializers import AuthTokenSerializer
from rest_framework.response import Response
from rest_framework.status import HTTP_201_CREATED, HTTP_401_UNAUTHORIZED, \
    HTTP_400_BAD_REQUEST, HTTP_404_NOT_FOUND, HTTP_403_FORBIDDEN, \
    HTTP_202_ACCEPTED
from rest_framework.views import APIView

from administration.notifications import send_html_email
from segment.models import SegmentChannel, SegmentVideo, SegmentKeyword
from userprofile.api.serializers import ContactFormSerializer, \
    ErrorReportSerializer
from userprofile.api.serializers import UserCreateSerializer, UserSerializer, \
    UserSetPasswordSerializer
from userprofile.models import UserProfile


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
        serializer = self.serializer_class(
            data=request.data, context={"request": request})
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
        token = request.data.get("token")
        auth_token = request.data.get("auth_token")
        if token:
            user = self.get_google_plus_user(token)
        elif auth_token:
            try:
                user = Token.objects.get(key=auth_token).user
            except Token.DoesNotExist:
                user = None
        else:
            serializer = AuthTokenSerializer(data=request.data)
            serializer.is_valid(raise_exception=True)
            user = serializer.validated_data['user']

        if not user:
            return Response(
                data={"error": ["Unable to authenticate user"
                                " with provided credentials"]},
                status=HTTP_400_BAD_REQUEST)

        Token.objects.get_or_create(user=user)
        update_last_login(None, user)
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


class UserProfileSharedListApiView(APIView):
    def get(self, request):
        user = request.user
        response = []

        # filter all user segments
        user_channel_segment = SegmentChannel.objects.filter(owner=user).values_list('shared_with', flat=True)
        user_video_segment = SegmentVideo.objects.filter(owner=user).values_list('shared_with', flat=True)
        user_keyword_segment = SegmentKeyword.objects.filter(owner=user).values_list('shared_with', flat=True)

        # build unique emails set
        unique_emails = set()
        for item in [user_channel_segment, user_video_segment, user_keyword_segment]:
            unique_emails |= set(chain.from_iterable(item))

        # collect required user data for each email
        for email in unique_emails:
            user_data = {}
            try:
                user = UserProfile.objects.get(email=email)
                user_data['email'] = user.email
                user_data['username'] = user.username
                user_data['registered'] = True
                user_data['date_joined'] = user.date_joined
                user_data['last_login'] = user.last_login

            except UserProfile.DoesNotExist:
                user_data['email'] = email
                user_data['registered'] = False

            response.append(user_data)

        return Response(data=response)


class UserPasswordResetApiView(APIView):
    """
    Password reset api view
    """
    permission_classes = tuple()

    def post(self, request):
        """
        Send email notification
        """
        email = request.data.get("email")
        try:
            user = get_user_model().objects.get(email=email)
        except get_user_model().DoesNotExist:
            return Response(status=HTTP_404_NOT_FOUND)
        if user.is_superuser:
            return Response(status=HTTP_403_FORBIDDEN)
        token = PasswordResetTokenGenerator().make_token(user)
        host = request.build_absolute_uri("/")
        reset_uri = "{host}password_reset/?email={email}&token={token}".format(
            host=host,
            email=email,
            token=token)
        subject = "SaaS > Password reset notification"
        text_header = "Dear {} \n".format(user.get_full_name())
        message = "Click the link below to reset your password.\n" \
                  "{}\n\n" \
                  "Please do not respond to this email.\n\n" \
                  "Kind regards, Channel Factory Team".format(reset_uri)
        send_html_email(
            subject, email, text_header, message, request.get_host())
        return Response(status=HTTP_202_ACCEPTED)


class UserPasswordSetApiView(APIView):
    """
    Endpoint for setting new password for user
    """
    permission_classes = tuple()
    serializer_class = UserSetPasswordSerializer

    def post(self, request):
        """
        Update user password
        """
        serializer = self.serializer_class(data=request.data)
        if not serializer.is_valid():
            return Response(serializer.errors, status=HTTP_400_BAD_REQUEST)
        email = serializer.data.get("email")
        token = serializer.data.get("token")
        try:
            user = get_user_model().objects.get(email=email)
        except get_user_model().DoesNotExist:
            return Response(status=HTTP_404_NOT_FOUND)
        if not PasswordResetTokenGenerator().check_token(user, token):
            return Response({"error": "Invalid link"}, HTTP_400_BAD_REQUEST)
        user.set_password(serializer.data.get("new_password"))
        user.save()
        return Response(status=HTTP_202_ACCEPTED)


class ContactFormApiView(APIView):
    """
    Admin emailing endpoint
    """
    serializer_class = ContactFormSerializer
    permission_classes = tuple()

    def post(self, request):
        """
        Email sending procedure
        """
        serializer = self.serializer_class(data=request.data)
        serializer.is_valid(raise_exception=True)
        sender = settings.SENDER_EMAIL_ADDRESS
        to = settings.CONTACT_FORM_EMAIL_ADDRESSES
        subject = "New request from contact form"
        text = "Dear Admin, \n\n" \
               "A new user has just filled contact from. \n\n" \
               "User email: {email} \n" \
               "User first name: {first_name} \n" \
               "User last name: {last_name} \n" \
               "User country: {country} \n" \
               "User company: {company}\n" \
               "User message: {message} \n\n".format(**serializer.data)
        send_mail(subject, text, sender, to, fail_silently=True)
        return Response(status=HTTP_201_CREATED)


class VendorDetailsApiView(APIView):
    """
    Endpoint to recognize server vendor
    """
    permission_classes = tuple()

    def get(self, request):
        """
        Get procedure
        """
        return Response(data={"vendor": settings.VENDOR})


class ErrorReportApiView(APIView):
    """
    Endpoint for sending error reports from UI
    """
    serializer_class = ErrorReportSerializer
    permission_classes = tuple()

    def post(self, request):
        """
        Send email procedure
        """
        serializer = self.serializer_class(data=request.data)
        serializer.is_valid(raise_exception=True)
        to = settings.CONTACT_FORM_EMAIL_ADDRESSES
        sender = settings.SENDER_EMAIL_ADDRESS
        host = request.get_host()
        subject = "UI Error Report from: {}".format(host)
        message = "User {email} has experienced an error on {host}: \n\n" \
                  "{message}".format(host=host, **serializer.data)
        send_mail(subject, message, sender, to, fail_silently=True)
        return Response(status=HTTP_201_CREATED)

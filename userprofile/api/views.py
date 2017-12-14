"""
Userprofile api views module
"""
import requests

from django.conf import settings
from django.contrib.auth import get_user_model
from django.contrib.auth.models import update_last_login
from django.contrib.auth.tokens import default_token_generator
from django.core.mail import send_mail
from rest_framework.authtoken.models import Token
from rest_framework.authtoken.serializers import AuthTokenSerializer
from rest_framework.response import Response
from rest_framework.status import HTTP_201_CREATED, HTTP_401_UNAUTHORIZED, \
    HTTP_400_BAD_REQUEST, HTTP_404_NOT_FOUND, HTTP_403_FORBIDDEN, HTTP_200_OK
from rest_framework.views import APIView

from userprofile.api.serializers import ContactFormSerializer
from userprofile.api.serializers import UserCreateSerializer, UserSerializer, \
    UserSetPasswordSerializer


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


class UserPasswordResetApiView(APIView):
    """
    Password reset api view
    """
    permission_classes = tuple()

    def post(self, request):
        """
        Send email notification
        """
        email = request.data.get('email')

        try:
            user = get_user_model().objects.get(email=email)
        except get_user_model().DoesNotExist:
            return Response({'email': email}, HTTP_404_NOT_FOUND)

        if user.is_superuser:
            return Response({'email': email}, HTTP_403_FORBIDDEN)

        token = default_token_generator.make_token(user)
        host = request.build_absolute_uri('/')

        reset_uri = '{host}password_reset?email={email}&token={token}'.format(
            host=host,
            email=email,
            token=token)

        user.email_user('SaaS > Password reset notification',
                        'SaaS system has received password reset request.\n'
                        'Click the link below to reset your password\n\n'
                        '{}\n\n'
                        'Please do not respond to this email.'
                        .format(reset_uri),
                        from_email=settings.SENDER_EMAIL_ADDRESS)

        return Response({'email': email}, HTTP_200_OK)


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
        if serializer.is_valid():
            email = serializer.data.get('email')
            token = serializer.data.get('token')

            try:
                user = get_user_model().objects.get(email=email)
            except get_user_model().DoesNotExist:
                return Response({'email': email}, HTTP_404_NOT_FOUND)

            if not default_token_generator.check_token(user, token):
                return Response({'token': 'Your link has expired. '
                                          'Please reset your password again.'},
                                HTTP_403_FORBIDDEN)
            user.set_password(serializer.data.get('new_password'))
            user.save()
            return Response(UserSerializer(user).data)
        return Response(serializer.errors, status=HTTP_400_BAD_REQUEST)


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

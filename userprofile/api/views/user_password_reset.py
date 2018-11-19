from django.contrib.auth import get_user_model
from django.contrib.auth.tokens import PasswordResetTokenGenerator
from rest_framework.response import Response
from rest_framework.status import HTTP_202_ACCEPTED
from rest_framework.status import HTTP_403_FORBIDDEN
from rest_framework.status import HTTP_404_NOT_FOUND
from rest_framework.views import APIView

from administration.notifications import send_html_email


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
        subject = "ViewIQ > Password reset notification"
        text_header = "Dear {} \n".format(user.get_full_name())
        message = "Click the link below to reset your password.\n" \
                  "{}\n\n" \
                  "Please do not respond to this email.\n\n" \
                  "Kind regards, Channel Factory Team".format(reset_uri)
        send_html_email(
            subject, email, text_header, message, request.get_host())
        return Response(status=HTTP_202_ACCEPTED)

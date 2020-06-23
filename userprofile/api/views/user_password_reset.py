from django.contrib.auth import get_user_model
from django.contrib.auth.tokens import PasswordResetTokenGenerator
from rest_framework.response import Response
from rest_framework.status import HTTP_202_ACCEPTED
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
            # for both cases below, we do not want to reveal whether or not a user exists, to protect against
            # brute-force attacks
            return Response(status=HTTP_202_ACCEPTED)
        if user.is_superuser:
            return Response(status=HTTP_202_ACCEPTED)
        token = PasswordResetTokenGenerator().make_token(user)
        host = request.build_absolute_uri("/")
        reset_uri = "{host}password_reset/?email={email}&token={token}".format(
            host=host,
            email=email,
            token=token)
        subject = "ViewIQ > Password reset notification"
        text_header = "Dear {} \n".format(user.get_full_name())
        message = "Click the link below to reset your password.\n" \
                  "{}" \
                  "<br><br>" \
                  "Please do not respond to this email".format(reset_uri)
        send_html_email(
            subject, email, text_header, message, host=host)
        return Response(status=HTTP_202_ACCEPTED)

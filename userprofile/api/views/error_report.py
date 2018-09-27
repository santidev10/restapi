from django.conf import settings
from django.core.mail import send_mail
from rest_framework.response import Response
from rest_framework.status import HTTP_201_CREATED
from rest_framework.views import APIView

from userprofile.api.serializers import ErrorReportSerializer


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

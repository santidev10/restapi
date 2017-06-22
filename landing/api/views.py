"""
Feedback api views module
"""
from django.conf import settings
from django.core.mail import send_mail
from rest_framework.response import Response
from rest_framework.status import HTTP_202_ACCEPTED, HTTP_200_OK
from rest_framework.views import APIView

from landing.api.serializers import ContactMessageSendSerializer
from landing.models import ContactMessage


class FeedbackSendApiView(APIView):
    """
    Send feedback endpoint
    """
    permission_classes = tuple()
    serializer_class = ContactMessageSendSerializer

    def get(self, request):
        return Response(status=HTTP_200_OK, data=settings.LANDING_SUBJECT)

    def post(self, request):
        """
        Process feedback 
        :param request: regular http(s) request
        :return: json response
        """
        serializer = self.serializer_class(data=request.data)
        serializer.is_valid(raise_exception=True)

        ContactMessage.objects.create(
            subject=serializer.data.get('subject'),
            email=serializer.data.get('email'),
            data=serializer.data)

        self.send_email(serializer.data)
        return Response(status=HTTP_202_ACCEPTED)

    def send_email(self, data):
        """
        Send new feedback
        :return: None 
        """

        sender = settings.SENDER_EMAIL_ADDRESS
        subject = data.get("subject", "default")
        to = ','.join(settings.LANDING_CONTACTS.get(subject))
        text = "Dear Manager, \n" \
               "You've got a new contact message sent via SaaS contact form. \n\n" \
               "From: {name} \n" \
               "Email: {email} \n" \
               "Company: {company}\n" \
               "Phone: {phone} \n\n" \
               "{message}".format(**data)
        send_mail(subject, text, sender, to, fail_silently=True)
        return

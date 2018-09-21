from django.conf import settings
from django.core.mail import send_mail
from rest_framework.response import Response
from rest_framework.status import HTTP_201_CREATED
from rest_framework.views import APIView

from userprofile.api.serializers import ContactFormSerializer


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

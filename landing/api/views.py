"""
Feedback api views module
"""
from django.conf import settings
from django.core.mail import send_mail
from rest_framework.response import Response
from rest_framework.status import HTTP_202_ACCEPTED, HTTP_200_OK, HTTP_408_REQUEST_TIMEOUT
from rest_framework.views import APIView

from landing.api.serializers import ContactMessageSendSerializer
from landing.models import ContactMessage
from singledb.connector import SingleDatabaseApiConnector as Connector, \
    SingleDatabaseApiConnectorException


class ContactMessageSendApiView(APIView):
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
        subject = data.get("subject")
        to = settings.LANDING_CONTACTS.get(subject, settings.LANDING_CONTACTS.get('default'))
        text = "Dear Manager, \n" \
               "You've got a new contact message sent via SaaS contact form. \n\n" \
               "From: {name} \n" \
               "Email: {email} \n" \
               "Company: {company}\n" \
               "Phone: {phone} \n\n" \
               "{message}".format(**data)
        send_mail(subject, text, sender, to, fail_silently=True)
        return


class TopAuthChannels(APIView):
    permission_classes = tuple()

    def get(self, request):
        connector = Connector()
        params_last_authed = dict(fields="channel_id,"
                                         "title,"
                                         "thumbnail_image_url,"
                                         "url,"
                                         "subscribers,"
                                         "auth__created_at",
                                  sort="auth__created_at:desc",
                                  sources="",
                                  auth__created_at__exists="true",
                                  subscribers__range="10000,",
                                  size="21")

        params_testimonials = dict(fields="channel_id,"
                                          "title,"
                                          "thumbnail_image_url,"
                                          "url,"
                                          "subscribers",
                                   sort="subscribers:desc",
                                   sources="",
                                   channel_id__terms=",".join(settings.TESTIMONIALS.keys()))
        try:
            channels_last_authed = connector.get_channel_list(params_last_authed)["items"]
            channels_testimonials = connector.get_channel_list(params_testimonials)["items"]
        except SingleDatabaseApiConnectorException as e:
            return Response(data={"error": " ".join(e.args)}, status=HTTP_408_REQUEST_TIMEOUT)

        for channel in channels_testimonials:
            channel_id = channel.get("channel_id")
            if channel_id in settings.TESTIMONIALS:
                channel["video_id"] = settings.TESTIMONIALS[channel_id]

        data = {"last": channels_last_authed,
                "testimonials": channels_testimonials}

        return Response(data)

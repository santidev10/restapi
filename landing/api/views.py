"""
Feedback api views module
"""
from django.conf import settings
from django.core.mail import send_mail
from rest_framework.response import Response
from rest_framework.status import HTTP_202_ACCEPTED, HTTP_200_OK, HTTP_408_REQUEST_TIMEOUT
from rest_framework.views import APIView

from channel.api.views import ChannelListApiView
from landing.api.serializers import ContactMessageSendSerializer
from landing.models import ContactMessage
from userprofile.models import UserProfile
from singledb.connector import SingleDatabaseApiConnector as Connector, \
    SingleDatabaseApiConnectorException


class ContanctMessageSendApiView(APIView):
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
        ids_from_request = request.query_params.get("ids")
        if ids_from_request:
            channels_ids = ids_from_request.split(",")
        else:
            channels_ids = list(UserProfile.objects.filter(
                channels__isnull=False).values_list(
                'channels__channel_id', flat=True))

        connector = Connector()
        try:
            ids_hash = connector.store_ids(channels_ids)
        except SingleDatabaseApiConnectorException as e:
            return Response(data={"error": " ".join(e.args)},
                            status=HTTP_408_REQUEST_TIMEOUT)

        fields = "channel_id,title,thumbnail_image_url,url,subscribers"
        query_params = dict(ids_hash=ids_hash,
                            fields=fields,
                            sort="subscribers:desc",
                            size="21")
        ChannelListApiView.adapt_query_params(query_params)

        try:
            channels = connector.get_channel_list(query_params)
        except SingleDatabaseApiConnectorException as e:
            return Response(data={"error": " ".join(e.args)},
                            status=HTTP_408_REQUEST_TIMEOUT)

        return Response(channels)

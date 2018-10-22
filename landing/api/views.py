"""
Feedback api views module
"""
from django.conf import settings
from rest_framework.response import Response
from rest_framework.status import HTTP_408_REQUEST_TIMEOUT
from rest_framework.views import APIView

from singledb.connector import SingleDatabaseApiConnector as Connector
from singledb.connector import SingleDatabaseApiConnectorException


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

        data = {
            "last": channels_last_authed,
            "testimonials": channels_testimonials
        }

        return Response(data)

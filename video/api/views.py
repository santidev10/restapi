"""
Video api views module
"""
from rest_framework.permissions import IsAuthenticated

from singledb.api.views.base import SingledbApiView
from singledb.connector import SingleDatabaseApiConnector as Connector


class VideoListApiView(SingledbApiView):
    connector_get = Connector().get_video_list


class VideoListFiltersApiView(SingledbApiView):
    connector_get = Connector().get_video_filters_list


class VideoRetrieveUpdateApiView(SingledbApiView):
    connector_get = Connector().get_video
    connector_put = Connector().put_video


class VideoSetApiView(SingledbApiView):
    connector_delete = Connector().delete_videos

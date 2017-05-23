"""
Video api views module
"""
from singledb.api.views.base import SingledbApiView
from singledb.connector import SingleDatabaseApiConnector as Connector
from utils.permissions import OnlyAdminUserCanCreateUpdateDelete


class VideoListApiView(SingledbApiView):
    connector_get = Connector().get_video_list


class VideoListFiltersApiView(SingledbApiView):
    connector_get = Connector().get_video_filters_list


class VideoRetrieveUpdateApiView(SingledbApiView):
    permission_classes = (IsAuthenticated, OnlyAdminUserCanCreateUpdateDelete)
    connector_get = Connector().get_video
    connector_put = Connector().put_video


class VideoSetApiView(SingledbApiView):
    permission_classes = (IsAuthenticated, OnlyAdminUserCanCreateUpdateDelete)
    connector_delete = Connector().delete_videos

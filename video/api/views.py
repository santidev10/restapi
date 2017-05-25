"""
Video api views module
"""
from django.db.models import Q
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.status import HTTP_404_NOT_FOUND, HTTP_408_REQUEST_TIMEOUT
from rest_framework.views import APIView

from segment.models import Segment
from singledb.api.views.base import SingledbApiView
from singledb.connector import SingleDatabaseApiConnector as Connector, \
    SingleDatabaseApiConnectorException
from utils.permissions import OnlyAdminUserCanCreateUpdateDelete


class VideoListApiView(APIView):
    """
    Proxy view for video list
    """
    def obtain_segment(self, segment_id):
        """
        Try to get segment from db
        """
        try:
            if self.request.user.is_staff:
                segment = Segment.objects.get(id=segment_id)
            else:
                segment = Segment.objects.filter(
                    Q(owner=self.request.user) |
                    ~Q(category="private")).get(id=segment_id)
        except Segment.DoesNotExist:
            return None
        return segment

    def get(self, request):
        # prepare query params
        query_params = request.query_params
        query_params._mutable = True
        # segment
        segment = query_params.get("segment")
        if segment is not None:
            # obtain segment
            segment = self.obtain_segment(segment)
            if segment is None:
                return Response(status=HTTP_404_NOT_FOUND)
            # obtain channels ids
            videos_ids = segment.videos.values_list(
                "video_id", flat=True)
            query_params.pop("segment")
            query_params.update(ids=",".join(videos_ids))
        # make call
        connector = Connector()
        try:
            response_data = connector.get_video_list(query_params)
        except SingleDatabaseApiConnectorException as e:
            return Response(
                data={"error": " ".join(e.args)},
                status=HTTP_408_REQUEST_TIMEOUT)
        return Response(response_data)


class VideoListFiltersApiView(SingledbApiView):
    connector_get = Connector().get_video_filters_list


class VideoRetrieveUpdateApiView(SingledbApiView):
    permission_classes = (IsAuthenticated, OnlyAdminUserCanCreateUpdateDelete)
    connector_get = Connector().get_video
    connector_put = Connector().put_video


class VideoSetApiView(SingledbApiView):
    permission_classes = (IsAuthenticated, OnlyAdminUserCanCreateUpdateDelete)
    connector_delete = Connector().delete_videos

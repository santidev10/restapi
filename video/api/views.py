"""
Video api views module
"""
from django.db.models import Q
from rest_framework.response import Response
from rest_framework.status import HTTP_404_NOT_FOUND, HTTP_408_REQUEST_TIMEOUT
from rest_framework.views import APIView

from segment.models import SegmentVideo
# pylint: disable=import-error
from singledb.api.views.base import SingledbApiView
from singledb.connector import SingleDatabaseApiConnector as Connector, \
    SingleDatabaseApiConnectorException
# pylint: enable=import-error
from utils.csv_export import list_export
from utils.permissions import OnlyAdminUserCanCreateUpdateDelete, \
    OnlyAdminUserOrSubscriber
from video.api.settings import DEFAULT_VIDEO_LIST_FIELDS


class VideoListApiView(APIView):
    """
    Proxy view for video list
    """
    # TODO Check additional auth logic
    permission_classes = (OnlyAdminUserOrSubscriber,)
    fields_to_export = [
        "title",
        "url",
        "views",
        "likes",
        "dislikes",
        "comments",
        "youtube_published_at"
    ]
    export_file_title = "video"

    def obtain_segment(self, segment_id):
        """
        Try to get segment from db
        """
        try:
            if self.request.user.is_staff:
                segment = SegmentVideo.objects.get(id=segment_id)
            else:
                segment = SegmentVideo.objects.filter(
                    Q(owner=self.request.user) |
                    ~Q(category="private")).get(id=segment_id)
        except SegmentVideo.DoesNotExist:
            return None
        return segment

    @list_export
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
            videos_ids = segment.get_related_ids()
            if not videos_ids:
                empty_response = {
                    "max_page": 1,
                    "items_count": 0,
                    "items": [],
                    "current_page": 1,
                }
                return Response(empty_response)
            query_params.pop("segment")
            query_params.update(ids=",".join(videos_ids))
        # fields
        if "fields" not in request.query_params:
            request.query_params["fields"] = ",".join(
                DEFAULT_VIDEO_LIST_FIELDS)
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
    permission_classes = (OnlyAdminUserOrSubscriber,)

    connector_get = Connector().get_video_filters_list


class VideoRetrieveUpdateApiView(SingledbApiView):
    permission_classes = (OnlyAdminUserOrSubscriber, OnlyAdminUserCanCreateUpdateDelete)
    connector_get = Connector().get_video
    connector_put = Connector().put_video


class VideoSetApiView(SingledbApiView):
    permission_classes = (OnlyAdminUserOrSubscriber, OnlyAdminUserCanCreateUpdateDelete)
    connector_delete = Connector().delete_videos

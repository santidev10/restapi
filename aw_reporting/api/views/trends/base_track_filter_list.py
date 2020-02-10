from rest_framework.response import Response

from aw_reporting.api.views.trends.base_track import TrackApiBase
from aw_reporting.tools.trends_tool.base_filters import BaseTrackFiltersList


class BaseTrackFiltersListApiView(TrackApiBase):
    """
    Lists of the filter names and values
    """
    filter_class = BaseTrackFiltersList

    def get(self, request, *args, **kwargs):
        filters = self.filter_class().get_filters(request.user)
        return Response(data=filters)

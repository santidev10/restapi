from aw_reporting.api.views.trends.base_track_filter_list import \
    BaseTrackFiltersListApiView

from aw_reporting.tools.trends_tool.track_filters import TrackFiltersList


class TrackFiltersListApiView(BaseTrackFiltersListApiView):
    """
    Lists of the filter names and values
    """
    filter_class = TrackFiltersList

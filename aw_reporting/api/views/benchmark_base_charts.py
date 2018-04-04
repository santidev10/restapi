from rest_framework.response import Response

from aw_reporting.api.views.base_track import TrackApiBase
from aw_reporting.benchmark import ChartsHandler


class BenchmarkBaseChartsApiView(TrackApiBase):
    """
    Return data for chart building
    """

    def get(self, request):
        ch = ChartsHandler(request=request)
        return Response(ch.base_charts())

from rest_framework.response import Response

from aw_reporting.api.views.trends.base_track import TrackApiBase
from aw_reporting.benchmark import ChartsHandler


class BenchmarkProductChartsApiView(TrackApiBase):
    """
    Return data for chart building
    """

    def get(self, request):
        ch = ChartsHandler(request=request)
        return Response(ch.product_charts())

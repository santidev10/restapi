
from rest_framework.views import APIView
from rest_framework.response import Response

from cache.models import CacheItem
from dashboard.utils import get_cache_key


class DashboardPacingAlertsAPIView(APIView):
    CACHE_PREFIX = "dashboard_pacing_alerts_"

    def get(self, request, *args, **kwargs):
        params = str(request.query_params) + str(kwargs)
        cache_key = get_cache_key(params, prefix=self.CACHE_PREFIX)
        try:
            # Possibly add some TTL to avoid retrieving stale data forever
            data = CacheItem.objects.get(key=cache_key).value
        except CacheItem.DoesNotExist:
            data = {}
            CacheItem.objects.create(key=cache_key, value=data)
        return Response(data=data)

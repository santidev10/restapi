from django.utils import timezone
from rest_framework.response import Response
from rest_framework.views import APIView

from aw_reporting.calculations.pacing_report_filters import get_pacing_report_filters
from cache.constants import PACING_REPORT_FILTERS_KEY
from cache.models import CacheItem


class PacingReportFiltersApiView(APIView):

    def get(self, request):
        try:
            cache_item = CacheItem.objects.get(key=PACING_REPORT_FILTERS_KEY)
            data = cache_item.value
        except CacheItem.DoesNotExist:
            data = get_pacing_report_filters()
            CacheItem.objects.update_or_create(
                key=PACING_REPORT_FILTERS_KEY,
                defaults={
                    'updated_at': timezone.now(),
                    'value': data,
                }
            )
        return Response(data)

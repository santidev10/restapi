from rest_framework.generics import ListAPIView
from rest_framework.response import Response

from aw_reporting.models import Topic, Audience, AdGroup, AgeRangeStatistic, \
    AgeRanges, GenderStatistic, Genders, AdGroupStatistic, Devices


class BenchmarkFiltersListApiView(ListAPIView):
    """
    Lists of the filter names and values
    """

    def get(self, request, *args, **kwargs):
        result = {}
        filters = request.query_params.get("filters", [])
        if "topics" in filters:
            result["topics"] = Topic.objects \
                .filter(parent__isnull=True) \
                .order_by("name") \
                .values("id", "name")
        if "interests" in filters:
            result["interests"] = Audience.objects \
                .filter(parent__isnull=True) \
                .order_by("name") \
                .values("id", "name")
        if "product_types" in filters:
            result["product_types"] = AdGroup.objects.all() \
                .values("type") \
                .distinct()
        if "age_range" in filters:
            age_range_query = AgeRangeStatistic.objects \
                .order_by() \
                .values("age_range_id") \
                .distinct()
            for age_range in age_range_query:
                age_range["name"] = AgeRanges[age_range["age_range_id"]]
            result["age_range"] = age_range_query
        if "gender" in filters:
            gender_query = GenderStatistic.objects \
                .order_by() \
                .values("gender_id") \
                .distinct()
            for gender in gender_query:
                gender["name"] = Genders[gender["gender_id"]]
            result["gender"] = gender_query
        if "device" in filters:
            device_query = AdGroupStatistic.objects \
                .order_by() \
                .values("device_id") \
                .distinct()
            for device in device_query:
                device["name"] = Devices[device["device_id"]]
            result["device"] = device_query
        return Response(data=result)

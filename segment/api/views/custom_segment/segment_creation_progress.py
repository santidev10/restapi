from rest_framework.response import Response
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_400_BAD_REQUEST
from rest_framework.status import HTTP_502_BAD_GATEWAY
from rest_framework.views import APIView

from brand_safety.utils import BrandSafetyQueryBuilder
from utils.elasticsearch import ElasticSearchConnectorException


class SegmentCreationProgressApiView(APIView):
    def get(self, request, *args, **kwargs):
        data = self._map_query_params(request.query_params)
        data["segment_type"] = kwargs["segment_type"]
        try:
            query_builder = BrandSafetyQueryBuilder(data)
        except KeyError:
            return Response(status=HTTP_400_BAD_REQUEST, data="You must provide a list_type.")
        try:
            result = query_builder.execute()
            data = {"items": result["hits"]["total"]}
            status = HTTP_200_OK
        except ElasticSearchConnectorException:
            status = HTTP_502_BAD_GATEWAY
            data = "Unable to connect to Elasticsearch."
        return Response(status=status, data=data)

    def _map_query_params(self, query_params):
        query_params._mutable = True
        try:
            query_params["languages"] = query_params["languages"].split(",")
        except KeyError:
            pass
        try:
            query_params["youtube_categories"] = query_params["youtube_categories"].split(",")
        except KeyError:
            pass
        try:
            query_params["brand_safety_categories"] = query_params["brand_safety_categories"].split(",")
        except KeyError:
            pass
        return query_params

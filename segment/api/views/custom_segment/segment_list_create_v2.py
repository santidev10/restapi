from rest_framework.response import Response
from rest_framework.status import HTTP_400_BAD_REQUEST
from rest_framework.status import HTTP_502_BAD_GATEWAY
from rest_framework.views import APIView

from brand_safety.utils import BrandSafetyQueryBuilder
from segment.api.views import SegmentListCreateApiView
from utils.elasticsearch import ElasticSearchConnectorException


class SegmentListCreateApiViewV2(SegmentListCreateApiView, APIView):
    REQUIRED_FIELDS = ["list_type", "languages", "score_threshold", "brand_safety_categories", "youtube_categories"]

    def post(self, request, *args, **kwargs):
        data = request.data
        try:
            self._validate_data(data)
        except ValueError:
            return Response(
                status=HTTP_400_BAD_REQUEST,
                data="You must provide the following fields: {}".format(", ".join(self.REQUIRED_FIELDS))
            )
        data["segment_type"] = self.model.segment_type
        query_builder = BrandSafetyQueryBuilder(data)
        try:
            result = query_builder.execute()
        except ElasticSearchConnectorException:
            return Response(status=HTTP_502_BAD_GATEWAY, data="Unable to connect to Elasticsearch.")

        return Response(result)

    def _validate_data(self, data):
        expected = set(self.REQUIRED_FIELDS)
        received = set(data.keys())
        if expected != received:
            raise ValueError


from rest_framework.response import Response
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_400_BAD_REQUEST
from rest_framework.status import HTTP_502_BAD_GATEWAY
from rest_framework.views import APIView

from audit_tool.models import AuditCategory
from brand_safety.models import BadWordCategory
from brand_safety.utils import BrandSafetyQueryBuilder
from utils.elasticsearch import ElasticSearchConnectorException
from segment.api.views import SegmentListCreateApiViewV2


class SegmentCreationOptionsApiView(APIView):
    OPTIONAL_FIELDS = ["brand_safety_categories", "languages", "list_type", "score_threshold", "youtube_categories"]

    def get(self, request, *args, **kwargs):
        data = self._map_query_params(request.query_params)
        try:
            self._validate_data(data)
        except ValueError as err:
            return Response(status=HTTP_400_BAD_REQUEST, data=str(err))
        data["segment_type"] = kwargs["segment_type"]
        query_builder = BrandSafetyQueryBuilder(data)
        try:
            result = query_builder.execute()
            data = {
                "items": result["hits"]["total"],
                "options": self._get_options()
            }
            status = HTTP_200_OK
        except ElasticSearchConnectorException:
            data = "Unable to connect to Elasticsearch."
            status = HTTP_502_BAD_GATEWAY
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
        try:
            query_params["score_threshold"] = int(query_params["score_threshold"])
        except KeyError:
            pass
        return query_params

    def _get_options(self):
        options = {
            "brand_safety_categories": BadWordCategory.get_category_mapping(),
            "youtube_categories": AuditCategory.get_all()
        }
        return options

    def _validate_data(self, data):
        expected = self.OPTIONAL_FIELDS
        received = data.keys()
        unexpected = any(key not in expected for key in received)
        if unexpected:
            err = "Unexpected fields: {}".format(", ".join(set(received) - set(expected)))
        else:
            err = SegmentListCreateApiViewV2.validate_threshold(data.get("score_threshold", 0))
        if err:
            raise ValueError(err)

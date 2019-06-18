from rest_framework.response import Response
from rest_framework.status import HTTP_201_CREATED
from rest_framework.status import HTTP_400_BAD_REQUEST
from rest_framework.views import APIView

from brand_safety.utils import BrandSafetyQueryBuilder
from segment.api.views import SegmentListCreateApiView
from segment.models.custom_segment_file_upload import CustomSegmentFileUpload


class SegmentListCreateApiViewV2(SegmentListCreateApiView, APIView):
    REQUIRED_FIELDS = ["list_type", "languages", "score_threshold", "brand_safety_categories", "youtube_categories"]

    def post(self, request, *args, **kwargs):
        data = request.data
        content_type = self.model.segment_type
        try:
            self._validate_data(data)
        except ValueError:
            return Response(
                status=HTTP_400_BAD_REQUEST,
                data="You must provide the following fields: {}".format(", ".join(self.REQUIRED_FIELDS))
            )
        data["segment_type"] = content_type
        query_builder = BrandSafetyQueryBuilder(data)
        filename = "testing"
        to_export = CustomSegmentFileUpload.enqueue(
            owner=request.user,
            query=query_builder.query_body,
            content_type=content_type,
            filename=filename
        )
        return Response(status=HTTP_201_CREATED, data=to_export.query)

    def _validate_data(self, data):
        expected = set(self.REQUIRED_FIELDS)
        received = set(data.keys())
        if expected != received:
            raise ValueError




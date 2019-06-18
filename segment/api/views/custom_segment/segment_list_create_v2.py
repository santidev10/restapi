from rest_framework.response import Response
from rest_framework.status import HTTP_201_CREATED
from rest_framework.status import HTTP_400_BAD_REQUEST

from brand_safety.utils import BrandSafetyQueryBuilder
from segment.api.views import SegmentListCreateApiView
from segment.models.custom_segment_file_upload import CustomSegmentFileUpload


class SegmentListCreateApiViewV2(SegmentListCreateApiView):
    REQUIRED_FIELDS = ["brand_safety_categories", "category", "languages", "list_type", "score_threshold", "title", "youtube_categories"]

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
        segment = super().post(request, *args, **kwargs)
        data["segment_type"] = content_type
        query_builder = BrandSafetyQueryBuilder(data)
        to_export = CustomSegmentFileUpload.enqueue(
            owner=request.user,
            query=query_builder.query_body,
        )
        segment.export = to_export
        segment.save()
        return Response(status=HTTP_201_CREATED, data=to_export.query)

    def _validate_data(self, data):
        expected = set(self.REQUIRED_FIELDS)
        received = set(data.keys())
        if expected != received:
            raise ValueError




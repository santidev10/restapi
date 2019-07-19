from rest_framework.generics import GenericAPIView
from rest_framework.response import Response
from rest_framework.status import HTTP_201_CREATED

from segment.api.mixins import DynamicModelViewMixin
from segment.api.serializers import SegmentSerializer


class SegmentDuplicateApiView(DynamicModelViewMixin, GenericAPIView):
    serializer_class = SegmentSerializer

    def post(self, request, pk):
        """
        Make a copy of segment and attach to user
        """
        segment = self.get_object()
        duplicated_segment = segment.duplicate(request.user)

        response_data = self.serializer_class(
            duplicated_segment,
            context={"request": request}
        ).data

        return Response(response_data, status=HTTP_201_CREATED)

from rest_framework.generics import RetrieveUpdateDestroyAPIView
from rest_framework.response import Response
from rest_framework.status import HTTP_403_FORBIDDEN

from segment.api.mixins import DynamicModelViewMixin
from segment.api.serializers import SegmentSerializer


class SegmentRetrieveUpdateDeleteApiView(DynamicModelViewMixin,
                                         RetrieveUpdateDestroyAPIView):
    serializer_class = SegmentSerializer

    def delete(self, request, *args, **kwargs):
        segment = self.get_object()
        user = request.user
        if not (user.is_staff or segment.owner == user):
            return Response(status=HTTP_403_FORBIDDEN)
        return super().delete(request, *args, **kwargs)

    def put(self, request, *args, **kwargs):
        """
        Allow partial update
        """
        segment = self.get_object()
        user = request.user

        if not (user.is_staff or segment.owner == user):
            return Response(status=HTTP_403_FORBIDDEN)

        serializer_context = {"request": request}

        serializer = self.serializer_class(
            instance=segment, data=request.data,
            context=serializer_context, partial=True
        )
        serializer.is_valid(raise_exception=True)
        segment = serializer.save()

        response_data = self.serializer_class(
            segment,
            context=serializer_context
        ).data
        return Response(response_data)

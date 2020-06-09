from rest_framework.generics import UpdateAPIView
from segment.api.serializers.custom_segment_update_serializer import CustomSegmentUpdateSerializer
from segment.models import CustomSegment


class CustomSegmentUpdateApiView(UpdateAPIView):

    def get_object(self):
        pk = self.kwargs.get('pk', None)
        return CustomSegment.objects.get(pk=pk)

    def get_serializer_class(self):
        return CustomSegmentUpdateSerializer

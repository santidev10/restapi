from rest_framework.generics import UpdateAPIView
from rest_framework.response import Response
from rest_framework.serializers import ValidationError
from rest_framework.status import HTTP_200_OK

from audit_tool.models import get_hash_name
from segment.api.serializers.custom_segment_serializer import CustomSegmentSerializer
from segment.models.custom_segment import CustomSegment
from utils.permissions import user_has_permission
from utils.permissions import or_permission_classes


class SegmentUpdateAPIView(UpdateAPIView):
    permission_classes = (
        or_permission_classes(
            user_has_permission("userprofile.vet_audit_admin"),
        ),
    )
    serializer_class = CustomSegmentSerializer

    def put(self, request, *args, **kwargs):
        data = request.data
        try:
            segment = CustomSegment.objects.get(id=kwargs["pk"])
        except CustomSegment.DoesNotExist:
            raise ValidationError(f"Custom segment with ID {kwargs['pk']} does not exist.")
        try:
            data["title_hash"] = get_hash_name(data["title"].lower().strip())
        except Exception:
            raise ValidationError(f"No new 'title' for Custom Segment given.")

        data["owner"] = segment.owner.id
        data["segment_type"] = segment.segment_type
        segment_serializer = self.serializer_class(instance=segment, data=data, partial=True)
        segment_serializer.is_valid(raise_exception=True)
        segment_serializer.save()
        return Response(segment_serializer.data, status=HTTP_200_OK)
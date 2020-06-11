from rest_framework.exceptions import PermissionDenied
from rest_framework.generics import UpdateAPIView
from rest_framework.permissions import IsAuthenticated

from segment.api.serializers.custom_segment_update_serializers import CustomSegmentAdminUpdateSerializer
from segment.api.serializers.custom_segment_update_serializers import CustomSegmentUpdateSerializer
from segment.models import CustomSegment


class CustomSegmentUpdateApiView(UpdateAPIView):

    permission_classes = (IsAuthenticated,)

    def get_object(self):
        pk = self.kwargs.get('pk', None)
        return CustomSegment.objects.get(pk=pk)

    def get_serializer(self, instance, *args, **kwargs):
        """
        Return the serializer instance that should be used for validating and
        deserializing input, and for serializing output.
        overwrites parent to pass instance to get_serializer_class so we can
        pass the correct serializer
        """
        serializer_class = self.get_serializer_class(instance)
        kwargs['context'] = self.get_serializer_context()
        return serializer_class(instance, *args, **kwargs)

    def get_serializer_class(self, instance):
        """
        return an update serializer based on user's permissions
        """
        if self.request.user.is_staff:
            return CustomSegmentAdminUpdateSerializer
        if self.request.user.id != instance.owner_id:
            raise PermissionDenied("You do not have sufficient privileges to modify this resource.")
        return CustomSegmentUpdateSerializer

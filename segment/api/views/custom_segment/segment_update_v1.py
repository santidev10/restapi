from rest_framework.exceptions import ValidationError
from rest_framework.generics import UpdateAPIView
from segment.api.serializers.custom_segment_update_serializers import CustomSegmentAdminUpdateSerializer
from segment.api.serializers.custom_segment_update_serializers import CustomSegmentUpdateSerializer
from segment.models import CustomSegment
from utils.permissions import user_has_permission


class CustomSegmentUpdateApiView(UpdateAPIView):

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
        if user_has_permission('userprofile.download_audit'):
            return CustomSegmentAdminUpdateSerializer
        if self.request.user != instance.owner:
            raise ValidationError("You do not have sufficient privileges to modify this resource.")
        return CustomSegmentUpdateSerializer

from rest_framework.generics import RetrieveUpdateDestroyAPIView
from rest_framework.permissions import IsAdminUser
from rest_framework.response import Response
from rest_framework.serializers import ValidationError

from brand_safety.api.serializers.bad_word_serializer import BadWordSerializer
from brand_safety.models import BadWord


class BadWordRecoverApiView(RetrieveUpdateDestroyAPIView):
    permission_classes = (IsAdminUser,)
    serializer_class = BadWordSerializer
    queryset = BadWord.objects.all()

    def get(self, request, *args, **kwargs):
        pk = kwargs.get('pk')
        try:
            instance = BadWord.all_objects.get(id=pk)
        except BadWord.DoesNotExist:
            raise ValidationError("BadWord object with id: '{}' does not exist.".format(pk))
        if instance.deleted_at:
            instance.deleted_at = None
            instance.save(update_fields=['deleted_at'])
        data = {
            "name": instance.name,
            "category": instance.category.id,
            "negative_score": instance.negative_score,
            "language": instance.language.language
        }
        return Response(data)


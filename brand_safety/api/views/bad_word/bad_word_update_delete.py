import string

from rest_framework.generics import RetrieveUpdateDestroyAPIView
from rest_framework.permissions import IsAdminUser
from rest_framework.response import Response
from rest_framework.serializers import ValidationError

from brand_safety.api.serializers.bad_word_serializer import BadWordSerializer
from brand_safety.models import BadWord
from audit_tool.models import AuditLanguage


class BadWordUpdateDeleteApiView(RetrieveUpdateDestroyAPIView):
    permission_classes = (IsAdminUser,)
    serializer_class = BadWordSerializer
    queryset = BadWord.objects.all()

    def put(self, request, *args, **kwargs):
        request.data['name'] = request.data['name'].strip().translate(str.maketrans('', '', string.punctuation))
        try:
            existing_word = BadWord.all_objects.get(name=request.data['name'],
                                                    language=AuditLanguage.from_string(request.data['language']))
            current_word = BadWord.objects.get(id=kwargs['pk'])
            # If attempting to change a word/language to one that is already in the database
            if existing_word.id != current_word.id:
                # If existing word has been soft deleted before
                if existing_word.deleted_at is not None:
                    existing_word.deleted_at = None
                    existing_word.save(update_fields=['deleted_at'])
                    existing_word_serializer = self.serializer_class(existing_word, data=request.data)
                    existing_word_serializer.is_valid(raise_exception=True)
                    existing_word_serializer.save()
                    current_word.delete()
                    return Response(existing_word_serializer.data)
                # If existing word is still in the database and hasn't been soft deleted
                else:
                    raise ValidationError("The word/language tag combination you are trying to update "
                                          "to already exists in the database.")
        except BadWord.DoesNotExist:
            pass
        return super().put(request, *args, **kwargs)
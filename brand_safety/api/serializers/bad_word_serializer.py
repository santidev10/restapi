from rest_framework.serializers import ModelSerializer
from rest_framework.serializers import ValidationError
from rest_framework.serializers import CharField

from brand_safety.models import BadWord, BadWordCategory


class BadWordSerializer(ModelSerializer):
    category_ref = CharField(max_length=80)

    def validate_category_ref(self, value):
        try:
            if type(value) is str:
                category_ref = BadWordCategory.from_string(value)
            else:
                try:
                    category_ref = BadWordCategory.objects.get(pk=value)
                except BadWordCategory.DoesNotExist:
                    raise ValidationError("Invalid category_ref value: {}".format(value))
            return category_ref
        except KeyError:
            raise ValidationError("category_ref required.")

    class Meta:
        model = BadWord
        fields = ("id", "name", "category_ref")
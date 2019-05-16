from rest_framework.serializers import ModelSerializer
from rest_framework.serializers import ValidationError
from rest_framework.serializers import CharField

from brand_safety.models import BadWord, BadWordCategory


class BadWordSerializer(ModelSerializer):
    category = CharField(max_length=80)

    def validate_category(self, value):
        try:
            if type(value) is str:
                category = BadWordCategory.from_string(value)
            else:
                try:
                    category = BadWordCategory.objects.get(pk=value)
                except BadWordCategory.DoesNotExist:
                    raise ValidationError("Invalid category value: {}".format(value))
            return category
        except KeyError:
            raise ValidationError("category required.")

    class Meta:
        model = BadWord
        fields = ("id", "name", "category", "negative_score")
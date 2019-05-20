from rest_framework.serializers import ModelSerializer
from rest_framework.serializers import ValidationError
from rest_framework.serializers import CharField

from brand_safety.models import BadWord, BadWordCategory


class BadWordSerializer(ModelSerializer):
    category = CharField(max_length=80)

    def validate_category(self, value):
        try:
            category_id = int(value)
            try:
                category = BadWordCategory.objects.get(pk=category_id)
            except BadWordCategory.DoesNotExist:
                raise ValidationError("Category with ID: {} does not exist. Please enter a valid category ID value.".format(value))
            return category
        except ValueError:
            raise ValidationError("Expected Category ID value. Received: {}".format(value))

    class Meta:
        model = BadWord
        fields = ("id", "name", "category", "negative_score")

from rest_framework.serializers import ModelSerializer
from rest_framework.serializers import ValidationError
from rest_framework.serializers import CharField

from brand_safety.models import BadWord, BadWordCategory


class BadWordSerializer(ModelSerializer):
    category = CharField(max_length=80)

    def validate_name(self, value):
        try:
            name = str(value).strip()
            return name
        except ValueError:
            raise ValidationError("Unable to process name: {}".format(value))

    def validate_category(self, value):
        try:
            category_id = int(value)
            try:
                category = BadWordCategory.objects.get(pk=category_id)
            except BadWordCategory.DoesNotExist:
                raise ValidationError("Category with ID: {} does not exist. Please enter a valid category ID."
                                      .format(value))
            return category
        except ValueError:
            raise ValidationError("Expected Category ID value. Received: {}".format(value))

    def validate_negative_score(self, value):
        try:
            score_val = int(value)
            if score_val < 1 or score_val > 4:
                raise ValidationError("Negative_score value: {} is out of range. Must be between 1-4.".format(value))
            return score_val
        except ValueError:
            raise ValidationError("Negative_score must be Integer with value between 1-4. Received: {}".format(value))

    class Meta:
        model = BadWord
        fields = ("id", "name", "category", "negative_score")

from rest_framework.serializers import ModelSerializer
from rest_framework.serializers import ValidationError
from rest_framework.serializers import CharField

from brand_safety.models import BadWord
from brand_safety.models import BadWordHistory


class BadWordHistorySerializer(ModelSerializer):
    tag = CharField(max_length=80)

    def validate_tag(self, value):
        try:
            tag_id = int(value)
        except ValueError:
            raise ValidationError("Expected BadWord ID value. Received: {}".format(value))

    class Meta:
        model = BadWordHistory
        fields = ("id", "tag", "action", "created_at")

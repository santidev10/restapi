from rest_framework.serializers import ModelSerializer

from brand_safety.models import BadWordCategory


class BadWordCategorySerializer(ModelSerializer):
    class Meta:
        model = BadWordCategory
        fields = ("id", "name")

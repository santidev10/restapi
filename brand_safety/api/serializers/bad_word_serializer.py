from rest_framework.serializers import ModelSerializer

from brand_safety.models import BadWord


class BadWordSerializer(ModelSerializer):
    class Meta:
        model = BadWord
        fields = ("id", "name", "category")

from rest_framework.serializers import ModelSerializer

from brand_safety.models import BadWordHistory

class BadWordsHistorySerializer(ModelSerializer):
    class Meta:
        model = BadWordHistory
        fields = ("id", "tag_name", "action", "created_at")

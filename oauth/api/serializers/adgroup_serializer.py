from rest_framework import serializers

from oauth.models import AdGroup


class AdGroupSerializer(serializers.ModelSerializer):
    class Meta:
        model = AdGroup
        fields = "__all__"

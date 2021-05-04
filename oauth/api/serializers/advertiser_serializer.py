from rest_framework import serializers

from oauth.models import DV360Advertiser


class AdvertiserSerializer(serializers.ModelSerializer):
    class Meta:
        model = DV360Advertiser
        fields = "__all__"

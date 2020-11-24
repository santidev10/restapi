from rest_framework import serializers

from performiq.models import Campaign


class CampaignSerializer(serializers.ModelSerializer):
    class Meta:
        model = Campaign
        fields = ["id", "account", "name"]

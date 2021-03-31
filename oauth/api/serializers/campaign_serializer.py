from rest_framework import serializers

from oauth.models import Campaign


class CampaignSerializer(serializers.ModelSerializer):

    name = serializers.SerializerMethodField()

    class Meta:
        model = Campaign
        fields = ["id", "account", "advertiser", "name"]

    def get_name(self, instance):
        # dv360 campaigns should use display_name instead of name
        return instance.display_name or instance.name
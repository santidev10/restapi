from rest_framework import serializers

from performiq.models import Campaign


class CampaignSerializer(serializers.ModelSerializer):

    name = serializers.SerializerMethodField()

    class Meta:
        model = Campaign
        fields = ["id", "account", "name"]

    def get_name(self, instance):
        # dv360 campaigns should use display_name instead of name
        return instance.display_name or instance.name
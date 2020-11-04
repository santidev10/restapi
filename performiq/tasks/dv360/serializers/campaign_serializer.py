from rest_framework import serializers

from performiq.models.models import Campaign


class CampaignSerializer(serializers.Serializer):
    id = serializers.IntegerField()
    advertiser_id = serializers.IntegerField()
    entity_status = serializers.IntegerField()
    name = serializers.CharField()
    display_name = serializers.CharField()
    update_time = serializers.DateTimeField()

    def save(self, **kwargs):
        campaign, _created = Campaign.objects.update_or_create(
            id=self.validated_data.get("id"),
            defaults=self.validated_data
        )
        return campaign

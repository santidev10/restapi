from rest_framework import serializers

from oauth.models import InsertionOrder
from oauth.constants import OAuthType


class InsertionOrderSerializer(serializers.Serializer):
    id = serializers.IntegerField()
    campaign_id = serializers.IntegerField()
    entity_status = serializers.IntegerField()
    name = serializers.CharField()
    display_name = serializers.CharField()
    update_time = serializers.DateTimeField()

    def save(self, **kwargs):
        campaign, _created = InsertionOrder.objects.update_or_create(
            id=self.validated_data.get("id"),
            defaults=self.validated_data
        )
        return campaign

from rest_framework import serializers

from oauth.models import DV360Advertiser


class AdvertiserSerializer(serializers.Serializer):
    id = serializers.IntegerField()
    partner_id = serializers.IntegerField()
    entity_status = serializers.IntegerField()
    name = serializers.CharField()
    display_name = serializers.CharField()
    update_time = serializers.DateTimeField()

    def save(self, **kwargs):
        advertiser, _created = DV360Advertiser.objects.update_or_create(
            id=self.validated_data.get("id"),
            defaults=self.validated_data
        )
        return advertiser

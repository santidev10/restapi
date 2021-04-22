from rest_framework import serializers

from oauth.constants import OAuthType
from oauth.models import AdGroup


class AdgroupSerializer(serializers.Serializer):
    id = serializers.IntegerField()
    line_item_id = serializers.IntegerField()
    entity_status = serializers.IntegerField()
    name = serializers.CharField()
    display_name = serializers.CharField()
    update_time = serializers.DateTimeField()

    def save(self, **kwargs):
        adgroup, _created = AdGroup.objects.update_or_create(
            id=self.validated_data.get("id"),
            oauth_type=OAuthType.DV360.value,
            defaults=self.validated_data
        )
        return adgroup

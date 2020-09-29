from rest_framework import serializers

from performiq.models.models import DV360Partner


class PartnerSerializer(serializers.Serializer):
    id = serializers.IntegerField()
    entity_status = serializers.IntegerField()
    name = serializers.CharField()
    display_name = serializers.CharField()
    update_time = serializers.DateTimeField()

    def save(self, **kwargs):
        partner, _created = DV360Partner.objects.update_or_create(
            id=self.validated_data.get("id"),
            defaults=self.validated_data
        )
        return partner

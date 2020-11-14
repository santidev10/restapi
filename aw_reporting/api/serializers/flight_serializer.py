from rest_framework import serializers
from rest_framework.exceptions import ValidationError

class FlightSerializer(serializers.Serializer):
    margin_cap = serializers.FloatField(required=False, allow_null=True)

    def validate_margin_cap(self, margin_cap):
        if margin_cap is None:
            return margin_cap
        margin_cap = float(margin_cap)
        if not 0 <= margin_cap <= 100:
            raise ValidationError("margin_cap must be between 0 and 100, inclusive.")
        return margin_cap

    def create(self, validated_data):
        raise NotImplementedError

    def update(self, instance, validated_data):
        instance.margin_cap = validated_data["margin_cap"]
        instance.save()
        return instance

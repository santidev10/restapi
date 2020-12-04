from rest_framework import serializers


class OAuthAccountSerializer(serializers.Serializer):
    is_enabled = serializers.BooleanField(required=False)

    def update(self, instance, validated_data):
        for key, value in validated_data.items():
            setattr(instance, key, value)
        instance.save()
        return instance

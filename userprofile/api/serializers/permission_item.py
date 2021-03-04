from rest_framework import serializers

from userprofile.models import PermissionItem


class PermissionItemSerializer(serializers.ModelSerializer):
    class Meta:
        model = PermissionItem
        fields = "__all__"

    def to_representation(self, instance):
        enabled_permissions = self.context.get("enabled_permissions", set())
        result = {
            "perm": instance.permission,
            "enabled": instance.permission in enabled_permissions or instance.default_value,
            "text": instance.display
        }
        return result

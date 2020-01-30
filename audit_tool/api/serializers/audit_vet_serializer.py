from rest_framework.serializers import ModelSerializer
from audit_tool.models import AuditVetItem


class AuditVetSerializer(ModelSerializer):
    class Meta:
        model = AuditVetItem
        fields = (
            "id",
            "created_at",
            "updated_at",
            "checked_out",
            "vetted",
            "approved",
            "category",
            "language",
            "suitable",
            "age_group",
            "gender",
            "channel_type",
            "is_monetized",
        )

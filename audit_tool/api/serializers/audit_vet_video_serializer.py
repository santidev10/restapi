from rest_framework.serializers import ModelSerializer
from rest_framework.serializers import RelatedField

from audit_tool.models import AuditChannelVet
from audit_tool.models import AuditVideoVet
from audit_tool.validators import AuditToolValidator
from aw_reporting.validators import AwReportingValidator


class AuditVideoVetSerializer(ModelSerializer):
    language = RelatedField(source="language", read_only=True)
    country = RelatedField(source="country", read_only=True)
    category = RelatedField(source="category.category_display_iab", read_only=True)

    class Meta:
        model = AuditVideoVet
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

    def save(self, **kwargs):
        """
        Save values on self and related model
        :param kwargs: dict
        :return:
        """
        pass

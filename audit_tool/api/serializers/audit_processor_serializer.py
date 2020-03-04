from rest_framework.serializers import JSONField
from rest_framework.serializers import ModelSerializer

from audit_tool.models import AuditProcessor


class AuditProcessorSerializer(ModelSerializer):
    params = JSONField()

    class Meta:
        model = AuditProcessor
        fields = ("params",)

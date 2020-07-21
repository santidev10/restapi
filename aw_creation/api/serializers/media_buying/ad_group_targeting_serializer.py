from rest_framework import serializers

from aw_reporting.models import AdGroupTargeting
from aw_reporting.models import TargetingStatusEnum


class AdGroupTargetingSerializer(serializers.ModelSerializer):
    type = serializers.StringRelatedField()
    status = serializers.SerializerMethodField()

    class Meta:
        model = AdGroupTargeting
        fields = (
            "ad_group_id",
            "type",
            "criteria",
            "is_negative",
            "status"
        )

    def get_status(self, obj):
        status = TargetingStatusEnum(obj.status).name
        return status

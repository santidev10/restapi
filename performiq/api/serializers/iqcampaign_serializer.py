from rest_framework import serializers

from audit_tool.models import AuditContentQuality
from audit_tool.models import AuditContentType
from performiq.analyzers.constants import DataSourceType
from performiq.models import Campaign
from performiq.models import IQCampaign
from segment.api.serializers.ctl_params_serializer import NullableListField
from segment.api.serializers.ctl_params_serializer import CoerceListMemberField
from utils.views import get_object


class IQCampaignSerializer(serializers.ModelSerializer):
    campaign_id = serializers.IntegerField(write_only=True, default=None)
    csv_s3_key = serializers.CharField(write_only=True, default=None, allow_null=True)
    csv_column_mapping = serializers.JSONField(write_only=True, default=None, allow_null=True)

    average_cpv = serializers.FloatField(write_only=True, allow_null=True)
    average_cpm = serializers.FloatField(write_only=True, allow_null=True)

    content_categories = NullableListField(write_only=True)
    content_quality = CoerceListMemberField(write_only=True, allow_null=True, valid_values=set(AuditContentQuality.to_str_with_unknown.keys()))
    content_type = CoerceListMemberField(write_only=True, allow_null=True, valid_values=set(AuditContentType.to_str_with_unknown.keys()))
    exclude_content_categories = NullableListField(write_only=True)
    languages = NullableListField(write_only=True)
    name = serializers.CharField(max_length=255)
    score_threshold = serializers.IntegerField(write_only=True, allow_null=True)
    video_view_rate = serializers.FloatField(write_only=True, allow_null=True)
    user_id = serializers.IntegerField(write_only=True)

    # These fields are unavailable for DV360 IQCampaigns as the API does not support retrieving these metrics
    ctr = serializers.FloatField(required=False, write_only=True, default=None, allow_null=True)
    active_view_viewability = serializers.FloatField(write_only=True, required=False, default=None, allow_null=True)
    video_quartile_100_rate = serializers.FloatField(write_only=True, required=False, default=None, allow_null=True)

    # Read only fields
    analysis_type = serializers.SerializerMethodField()

    class Meta:
        model = IQCampaign
        fields = "__all__"

    def create(self, validated_data):
        campaign_id = validated_data.pop("campaign_id", None)
        user_id = validated_data.pop("user_id")
        iq_campaign = IQCampaign.objects.create(
            name=validated_data["name"],
            user_id=user_id,
            campaign_id=campaign_id,
            params=validated_data
        )
        return iq_campaign

    def validate_campaign_id(self, val):
        if val is not None:
            campaign_id = get_object(Campaign, id=val).id
        else:
            campaign_id = None
        return campaign_id

    def validate_content_quality(self, val):
        validated = [str(val) for val in super().validate(val)]
        return validated

    def validate_content_type(self, val):
        validated = [str(val) for val in super().validate(val)]
        return validated

    def get_analysis_type(self, obj) -> int:
        """
        Get analysis type of IQCampaign
        :param obj:
        :return:
        """
        if obj.params.get("csv_s3_key"):
            analysis_type = DataSourceType(2).value
        else:
            analysis_type = DataSourceType(obj.campaign.oauth_type).value
        return analysis_type

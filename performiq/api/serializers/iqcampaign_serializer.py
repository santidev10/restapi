from django.contrib.auth import get_user_model
from rest_framework import serializers

from audit_tool.models import AuditContentQuality
from audit_tool.models import AuditContentType
from performiq.models import Campaign
from performiq.models import IQCampaign
import performiq.tasks.start_analysis as start_analysis
from segment.api.serializers.ctl_params_serializer import NullableListField
from segment.api.serializers.ctl_params_serializer import CoerceListMemberField
from utils.views import get_object


class IQCampaignSerializer(serializers.ModelSerializer):
    campaign_id = serializers.IntegerField(write_only=True, default=None)
    csv_s3_key = serializers.CharField(write_only=True, default=None)
    csv_column_mapping = serializers.JSONField(write_only=True, default=None)

    average_cpv = serializers.FloatField(write_only=True, allow_null=True)
    average_cpm = serializers.FloatField(write_only=True, allow_null=True)

    content_categories = NullableListField(write_only=True)
    content_quality = CoerceListMemberField(write_only=True, allow_null=True, valid_values=set(AuditContentQuality.to_str_with_unknown.keys()))
    content_type = CoerceListMemberField(write_only=True, allow_null=True, valid_values=set(AuditContentType.to_str_with_unknown.keys()))
    exclude_content_categories = NullableListField(write_only=True)
    languages = NullableListField(write_only=True)
    score_threshold = serializers.IntegerField(write_only=True, allow_null=True)
    video_view_rate = serializers.FloatField(write_only=True, allow_null=True)
    user = serializers.PrimaryKeyRelatedField(default=None, queryset=get_user_model().objects.all())

    # These fields are unavailable for DV360 IQCampaigns as the API does not support retrieving these metrics
    ctr = serializers.FloatField(required=False, write_only=True, default=None, allow_null=True)
    active_view_viewability = serializers.FloatField(write_only=True, required=False, default=None, allow_null=True)
    video_quartile_100_rate = serializers.FloatField(write_only=True, required=False, default=None, allow_null=True)

    class Meta:
        model = IQCampaign
        fields = "__all__"

    def create(self, validated_data):
        campaign_id = validated_data.pop("campaign_id", None)
        # Only set user if IQCampaign is created from csv
        user = validated_data.pop("user")
        if not validated_data.get("csv_s3_key"):
            user = None
        iq_campaign = IQCampaign.objects.create(
            user=user,
            campaign_id=campaign_id,
            params=validated_data
        )
        start_analysis.start_analysis_task.delay(iq_campaign.id)
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

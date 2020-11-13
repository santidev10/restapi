from rest_framework import serializers
from audit_tool.models import AuditContentQuality
from audit_tool.models import AuditContentType

from segment.api.serializers.ctl_params_serializer import AdsPerformanceRangeField
from segment.api.serializers.ctl_params_serializer import NullableListField
from segment.api.serializers.ctl_params_serializer import CoerceListMemberField
from performiq.models import Campaign
from performiq.models import IQCampaign
from utils.views import get_object


class IQCampaignSerializer(serializers.ModelSerializer):
    campaign_id = serializers.IntegerField()
    average_cpm = AdsPerformanceRangeField(write_only=True)
    average_cpv = AdsPerformanceRangeField(write_only=True)
    video_view_rate = serializers.FloatField(write_only=True)
    content_categories = NullableListField(write_only=True)
    content_quality = CoerceListMemberField(write_only=True, valid_values=set(AuditContentQuality.to_str_with_unknown.keys()))
    content_type = CoerceListMemberField(write_only=True, valid_values=set(AuditContentType.to_str_with_unknown.keys()))
    exclude_content_categories = NullableListField(write_only=True)
    score_threshold = serializers.IntegerField(write_only=True)
    # These fields are unavailable for DV360 IQCampaigns as the API does not support retrieving these metrics
    ctr = AdsPerformanceRangeField(required=False)
    active_view_viewability = serializers.FloatField(write_only=True, required=False)
    video_quartile_100_rate = serializers.FloatField(write_only=True, required=False)

    class Meta:
        model = IQCampaign
        fields = "__all__"

    def create(self, validated_data):
        campaign_id = validated_data.pop("campaign_id")
        iq_campaign = IQCampaign.objects.create(
            campaign_id=campaign_id,
            params=validated_data
        )

    def get_campaign_id(self, val):
        campaign = get_object(Campaign, id=val)
        return campaign.id

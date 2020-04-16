from rest_framework.exceptions import ValidationError
from rest_framework import serializers

from aw_creation.models import AccountCreation
from aw_creation.models import AdCreation
from aw_creation.models import AdGroupCreation
from aw_creation.models import CampaignCreation
from aw_creation.models.creation import VideoUrlValidator
from aw_creation.models.creation import TrackingTemplateValidator
from aw_creation.api.serializers.serializers import CampaignCreationSetupSerializer
from aw_creation.api.serializers.serializers import AdGroupCreationSetupSerializer
from aw_creation.api.serializers.serializers import AdCreationSetupSerializer


class CampaignBreakoutSerializer(serializers.Serializer):
    CAMPAIGN_FIELDS = ("campaign_name", "budget", "start_date", "end_date")
    AD_GROUP_FIELDS = ("max_rate", "video_ad_format")
    AD_FIELDS = ("display_url", "final_url", "video_url", "tracking_template")

    campaign_name = serializers.CharField(max_length=255)
    end_date = serializers.DateField()
    start_date = serializers.DateField()
    budget = serializers.DecimalField(max_digits=10, decimal_places=2)

    video_ad_format = serializers.CharField(max_length=20)
    max_rate = serializers.DecimalField(max_digits=10, decimal_places=2)

    tracking_template = serializers.CharField(max_length=250, validators=[TrackingTemplateValidator], default="")
    video_url = serializers.URLField(validators=[VideoUrlValidator])
    final_url = serializers.URLField(validators=[VideoUrlValidator])
    display_url = serializers.URLField(validators=[VideoUrlValidator])

    def validate_video_ad_format(self, value):
        choices = set(dict(AdGroupCreation.VIDEO_AD_FORMATS).keys())
        if value not in dict(AdGroupCreation.VIDEO_AD_FORMATS):
            raise ValidationError(f"Invalid video_ad_format: {value}. Choices: {choices}")
        return value

    def create(self, validated_data):
        # For campaign FK, should create here or push to google ads only link when created on google ads?
        campaign_data = [validated_data[key] for key in self.CAMPAIGN_FIELDS]
        ad_group_data = [validated_data[key] for key in self.AD_GROUP_FIELDS]
        ad_data = [validated_data[key] for key in self.AD_FIELDS]

        campaign_serializer = CampaignCreationSetupSerializer(data=campaign_data)
        campaign_serializer.is_valid(raise_exception=True)
        campaign_creation = campaign_serializer.save()

        ad_group_serializer = AdGroupCreationSetupSerializer(data=ad_group_data)
        ad_group_serializer.is_valid(raise_exception=True)
        ad_group_serializer.save()

        ad_serializer = AdCreationSetupSerializer(data=ad_data)
        ad_serializer.is_valid(raise_exception=True)
        ad_serializer.save()
        return campaign_creation

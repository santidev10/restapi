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
    CAMPAIGN_FIELDS = ("name", "budget", "start", "end")
    AD_GROUP_FIELDS = ("name", "max_rate", "video_ad_format")
    AD_FIELDS = ("name", "display_url", "final_url", "video_url", "tracking_template")

    name = serializers.CharField(max_length=255)
    start = serializers.DateField()
    end = serializers.DateField()
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

    def validate(self, data, raise_exception=True):
        validated = {}
        try:
            validated["campaign_data"] = {key: data[key] for key in self.CAMPAIGN_FIELDS}
            validated["campaign_data"]["account_creation"] = self.context["account_creation"]
            validated["ad_group_data"] = {key: data[key] for key in self.AD_GROUP_FIELDS}
            validated["ad_data"] = {key: data[key] for key in self.AD_FIELDS}
        except KeyError as e:
            if raise_exception:
                raise ValidationError(e)
        return validated

    def create(self, validated_data):
        campaign_data = validated_data["campaign_data"]
        ad_group_data = validated_data["ad_group_data"]
        ad_data = validated_data["ad_data"]

        campaign_creation = CampaignCreation.objects.create(**campaign_data)
        ad_group_data.update({"campaign_creation": campaign_creation})
        ad_group_creation = AdGroupCreation.objects.create(**ad_group_data)
        ad_data.update({"ad_group_creation": ad_group_creation})
        ad_creation = AdCreation.objects.create(**ad_data)
        return campaign_creation

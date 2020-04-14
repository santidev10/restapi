from rest_framework.exceptions import ValidationError
from rest_framework import serializers

from aw_creation.models import AccountCreation
from aw_creation.models import AdCreation
from aw_creation.models import AdGroupCreation
from aw_creation.models import CampaignCreation
from aw_creation.models.creation import VideoUrlValidator
from aw_creation.models.creation import TrackingTemplateValidator
from aw_creation.api.serializers.serializers import AdCreationSetupSerializer


class CampaignBreakoutSerializer(serializers.Serializer):
    AD_FIELDS = ("video_ur", "display_url", "final_url", "companion_banner")
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
    companion_banner = serializers.ImageField(upload_to='img/custom_video_thumbs', blank=True, null=True)

    def validate_video_ad_format(self, value):
        choices = set(dict(AdGroupCreation.VIDEO_AD_FORMATS).keys())
        if value not in dict(AdGroupCreation.VIDEO_AD_FORMATS):
            raise ValidationError(f"Invalid video_ad_format: {value}. Choices: {choices}")
        return value

    def create(self, validated_data):
        account_creation = self.context["account_creation"]
        # For campaign FK, should create here or push to google ads only
        # link when created on google ads?

        ad_data = [validated_data[key] for key in self.AD_FIELDS]
        # campaign_creation = CampaignCreation.objects.create(
        #     account_creation=account_creation,
        #     start=validated_data["start_date"],
        #     end=validated_data["end_date"],
        #     budget=validated_data["budget"],
        #     # campaign_name?
        # )
        # ad_group_creation = AdGroupCreation.objects.create(
        #     max_rate=validated_data["max_rate"],
        #     campaign_creation=campaign_creation,
        #     video_ad_format=validated_data["video_ad_format"],
        # )
        ad_creation = AdCreationSetupSerializer(data=ad_data)
        ad_creation.is_valid()
        ad_creation.save()
        # ad_creation = AdCreation.objects.create(
        #     ad_group_creation=ad_group_creation,
        #     video_url=validated_data["video_url"],
        #     display_url=validated_data["display_url"],
        #     final_url=validated_data["final_url"],
        #     companion_banner=validated_data["companion_banner"],
        # )


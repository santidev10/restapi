from rest_framework.exceptions import ValidationError
from rest_framework import serializers

from aw_creation.models import AccountCreation
from aw_creation.models import AdCreation
from aw_creation.models import AdGroupCreation
from aw_creation.models import CampaignCreation
from aw_creation.models.creation import VideoUrlValidator
from aw_creation.models.creation import TrackingTemplateValidator
from aw_reporting.models import AdGroup


class CampaignBreakoutSerializer(serializers.Serializer):
    CAMPAIGN_FIELDS = ("name", "budget", "start", "end")
    AD_GROUP_FIELDS = ("name", "max_rate", "video_ad_format")
    AD_FIELDS = ("name", "display_url", "final_url", "video_url", "tracking_template")

    name = serializers.CharField(max_length=255)
    start = serializers.DateField()
    end = serializers.DateField()
    budget = serializers.DecimalField(max_digits=10, decimal_places=2)
    ad_group_ids = serializers.ListField()

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
        validated = {
            "campaign_data": {},
            "ad_group_data": [],
            "ad_data": {},
        }
        ag_types = set(AdGroup.objects.filter(id__in=data["ad_group_ids"]).values_list("campaign__type", flat=True))
        if len(ag_types) > 1:
            raise ValidationError(f"AdGroups to break out must all be of the same Campaign type. Received: {', '.join(ag_types)}")
        try:
            validated["campaign_data"] = {key: data[key] for key in self.CAMPAIGN_FIELDS}
            validated["campaign_data"]["account_creation"] = self.context["account_creation"]

            for ad_group_id in data["ad_group_ids"]:
                ag_data = {key: data[key] for key in self.AD_GROUP_FIELDS}
                ag_data["ad_group_id"] = ad_group_id
                validated["ad_group_data"].append(ag_data)
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
        ad_group_creation = AdGroupCreation.objects\
            .bulk_create([AdGroupCreation(campaign_creation=campaign_creation, **data) for data in ad_group_data])

        ad_creation = AdCreation.objects\
            .bulk_create([AdCreation(ad_group_creation=ag_creation, **ad_data) for ag_creation in ad_group_creation])
        return campaign_creation

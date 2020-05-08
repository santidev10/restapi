from django.db.models import F
from django.db.models import Value
from django.db.models.functions import Concat
from rest_framework.exceptions import ValidationError
from rest_framework import serializers

from aw_creation.models import AdGroupCreation
from aw_creation.models import CampaignCreation
from aw_reporting.models import AdGroup


class CampaignBreakoutSerializer(serializers.Serializer):
    CAMPAIGN_FIELDS = ("name", "budget", "start", "end")
    name = serializers.CharField(max_length=255)
    start = serializers.DateField()
    end = serializers.DateField()
    budget = serializers.DecimalField(max_digits=10, decimal_places=2)
    ad_group_ids = serializers.ListField()
    max_rate = serializers.DecimalField(max_digits=10, decimal_places=2)

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
            raise ValidationError(f"AdGroups to break out must all be of the same Campaign type. "
                                  f"Received: {', '.join(ag_types)}")
        try:
            validated["campaign_data"] = {key: data[key] for key in self.CAMPAIGN_FIELDS}
            validated["campaign_data"]["account_creation"] = self.context["account_creation"]
            ad_group_id_name_mapping = {
                ag.id: ag.name for ag in AdGroup.objects.filter(id__in=data["ad_group_ids"])
            }
            for ad_group_id in data["ad_group_ids"]:
                ag_data = {
                    "max_rate": data["max_rate"],
                    "ad_group_id": ad_group_id,
                    "name": ad_group_id_name_mapping[ad_group_id]
                }
                validated["ad_group_data"].append(ag_data)
        except KeyError as e:
            if raise_exception:
                raise ValidationError(e)
        return validated

    def create(self, validated_data):
        campaign_data = validated_data["campaign_data"]
        ad_group_data = validated_data["ad_group_data"]

        campaign_creation = CampaignCreation.objects.create(**campaign_data)
        # Update name with CampaignCreation id suffix
        campaign_creation.name += f" # {campaign_creation.id}"
        campaign_creation.save()

        AdGroupCreation.objects\
            .bulk_create([AdGroupCreation(campaign_creation=campaign_creation, **data) for data in ad_group_data])
        # Update new AdGroupCreation names with id suffix for identification and matching with Google Ads
        AdGroupCreation.objects.filter(name__in=[item["name"] for item in ad_group_data])\
            .update(name=Concat(F("name"), Value(" #"), F("id")))
        return campaign_creation

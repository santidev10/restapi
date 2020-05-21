from django.db.models import F
from django.db.models import Value
from django.db.models.functions import Concat
from rest_framework.exceptions import ValidationError
from rest_framework import serializers

from aw_creation.models import AdGroupCreation
from aw_creation.models import CampaignCreation
from aw_reporting.models import AdGroup
from aw_creation.api.views.media_buying.constants import CampaignBidStrategyTypeEnum
from aw_creation.api.views.media_buying.constants import AD_GROUP_TYPE_CAMPAIGN_BID_TYPE


class CampaignBreakoutSerializer(serializers.Serializer):
    CAMPAIGN_CREATION_FIELDS = ("name", "budget", "start", "end", "bid_strategy_type", "type")

    name = serializers.CharField(max_length=255)
    start = serializers.DateField()
    end = serializers.DateField()
    budget = serializers.DecimalField(max_digits=10, decimal_places=2)
    ad_group_ids = serializers.ListField(child=serializers.IntegerField())
    max_rate = serializers.DecimalField(max_digits=10, decimal_places=2)
    bidding_strategy_type = serializers.CharField(max_length=10, default=CampaignCreation.MAX_CPV_STRATEGY)

    def validate(self, data, raise_exception=True):
        validated = {
            "campaign_data": {},
            "ad_group_data": [],
            "ad_data": {},
        }
        ad_groups = AdGroup.objects\
            .filter(id__in=data["ad_group_ids"])\
            .annotate(campaign_type=F("campaign__type"))
        ag_type = set(ad_groups.values_list("type", flat=True))
        campaign_type = set(ad_groups.values_list("campaign_type", flat=True))
        if len(ag_type) > 1:
            raise ValidationError(f"AdGroups to break out must all be of the same type. "
                                  f"Received: {', '.join(ag_type)}")
        if len(campaign_type) > 1:
            raise ValidationError(f"AdGroup Campaign types must either be all Video or Display.")
        ag_type = list(ag_type)[0]
        campaign_type = list(campaign_type)[0]
        data["type"] = campaign_type.upper()
        try:
            ad_group_id_name_mapping = {
                ag.id: ag for ag in AdGroup.objects.filter(id__in=data["ad_group_ids"])
            }
            for ad_group_id in data["ad_group_ids"]:
                ag_data = {
                    "max_rate": data["max_rate"],
                    "ad_group_id": ad_group_id,
                    "name": ad_group_id_name_mapping[ad_group_id].name + " - BR"
                }
                validated["ad_group_data"].append(ag_data)
        except KeyError as e:
            if raise_exception:
                raise ValidationError(e)

        # Need to check exactly what bid strategy type to use since the Adwords API does not differentiate from
        # target cpm or max cpm strategies in Campaign Performance report. Must imply from the source ad groups
        mapping_value = AD_GROUP_TYPE_CAMPAIGN_BID_TYPE[ag_type]
        campaign_bidding_strategy_type = CampaignBidStrategyTypeEnum[mapping_value].value
        data["bid_strategy_type"] = campaign_bidding_strategy_type

        validated["campaign_data"] = {key: data[key] for key in self.CAMPAIGN_CREATION_FIELDS}
        if ag_type == "Standard":
            validated["campaign_data"]["sub_type"] = "Non-skippable"
        validated["campaign_data"]["account_creation"] = self.context["account_creation"]
        return validated

    def create(self, validated_data):
        campaign_data = validated_data["campaign_data"]
        ad_group_data = validated_data["ad_group_data"]

        campaign_creation = CampaignCreation.objects.create(**campaign_data)
        # Update name with CampaignCreation id suffix
        campaign_creation.name += f" #{campaign_creation.id}"
        campaign_creation.save()

        ad_group_creations = [AdGroupCreation(campaign_creation=campaign_creation, **data) for data in ad_group_data]
        AdGroupCreation.objects\
            .bulk_create(ad_group_creations)
        # Update new AdGroupCreation names with id suffix for identification and matching with Google Ads
        AdGroupCreation.objects.filter(name__in=[item["name"] for item in ad_group_data])\
            .update(name=Concat(F("name"), Value(" #"), F("id")))
        return campaign_creation

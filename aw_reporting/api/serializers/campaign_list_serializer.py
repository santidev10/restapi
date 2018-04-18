import re

from rest_framework.fields import SerializerMethodField
from rest_framework.serializers import ModelSerializer

from aw_reporting.api.serializers.simple_serializers import \
    AdGroupListSerializer
from aw_reporting.models import Campaign


class CampaignListSerializer(ModelSerializer):
    ad_groups = AdGroupListSerializer(many=True)
    campaign_creation_id = SerializerMethodField()

    def get_campaign_creation_id(self, obj):
        cid_search = re.match(r"^.*#(\d+)$", obj.name)
        if cid_search:
            campaign_creation_id = int(cid_search.group(1))
            if campaign_creation_id in self.campaign_creation_ids:
                return campaign_creation_id

    class Meta:
        model = Campaign
        fields = (
            "id", "name", "ad_groups", "status", "start_date", "end_date",
            "campaign_creation_id",
        )

    def __init__(self, *args, campaign_creation_ids=None, **kwargs):
        self.campaign_creation_ids = campaign_creation_ids
        super(CampaignListSerializer, self).__init__(*args, **kwargs)

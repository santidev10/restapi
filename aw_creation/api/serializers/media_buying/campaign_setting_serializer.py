from rest_framework import serializers

from aw_reporting.models import Campaign
from aw_reporting.models.salesforce_constants import SalesForceGoalType


class CampaignSettingSerializer(serializers.ModelSerializer):
    max_bid = serializers.SerializerMethodField()
    ad = serializers.SerializerMethodField()

    class Meta:
        model = Campaign
        fields = (
            "id",
            "name",
            "start_date",
            "end_date",
            "budget",
            "max_bid",
        )

    def get_max_bid(self, obj):
        """
        Get max bid setting from AdGroup context[bid_mapping] data
        :param obj: Campaign with salesforce_placement__goal_type_id annotation
        :return:
        """
        max_bid = None
        try:
            if obj.salesforce_goal_id == SalesForceGoalType.CPM:
                max_bid = self.context["bid_mapping"][obj.id]["cpm_bid"]
            else:
                max_bid = self.context["bid_mapping"][obj.id]["cpv_bid"]
        except KeyError:
            pass
        return max_bid

    def get_ad(self, obj):
        """
        Retrieve single Ad for settings serialization
        Usually ads are the same for an entire Campaign
        :param obj:
        :return:
        """
        ad_creation = obj.ads.all().first().ad_creation.values().first()
        return ad_creation

from rest_framework import serializers

from aw_reporting.models import Campaign
from aw_reporting.models.ad_words import VideoCreative
from aw_reporting.models.salesforce_constants import SalesForceGoalType


class CampaignSettingSerializer(serializers.ModelSerializer):
    max_bid = serializers.SerializerMethodField()
    ad = serializers.SerializerMethodField()

    class Meta:
        model = Campaign
        fields = (
            "id",
            "ad",
            "bidding_strategy_type",
            "name",
            "start_date",
            "end_date",
            "budget",
            "max_bid",
            "type",
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
            max_bid /= 1000000
        except (KeyError, TypeError):
            pass
        return max_bid

    def get_ad(self, obj):
        """
        Retrieve single Ad for settings serialization
        Usually ads are the same for an entire Campaign
        :param obj:
        :return:
        """
        ad = {}
        for ad_group in obj.ad_groups.all():
            try:
                video_id = VideoCreative.objects.filter(statistics__ad_group=ad_group).first().id
                ad_obj = ad_group.ads.first()
                ad.update({
                    "headline": ad_obj.headline,
                    "creative_name": ad_obj.creative_name,
                    "display_url": ad_obj.display_url,
                    "youtube_url": f"https://www.youtube.com/watch?v={video_id}"
                })
                break
            except AttributeError:
                continue
        return ad

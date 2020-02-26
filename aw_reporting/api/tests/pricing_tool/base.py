import json
from datetime import timedelta

from django.utils import timezone
from aw_reporting.models import SalesForceGoalType, Opportunity, OpPlacement, \
    Account, Campaign, AdGroup, GeoTarget, Category, CampaignStatistic, Topic, \
    TopicStatistic, AdGroupStatistic, Audience, AudienceStatistic, \
    VideoCreative, VideoCreativeStatistic, Genders, AgeRanges, \
    Flight, GeoTargeting, device_str, Device
from utils.unittests.test_case import ExtendedAPITestCase as APITestCase


class PricingToolTestCaseBase(APITestCase):
    _url = None

    def _request(self, **kwargs):
        return self.client.post(self._url,
                                json.dumps(kwargs),
                                content_type="application/json")

    def setUp(self):
        self.user = self.create_test_user()

    @staticmethod
    def _create_opportunity_campaign(_id, goal_type=SalesForceGoalType.CPV,
                                     opp_data=None, pl_data=None,
                                     camp_data=None, generate_statistic=True):
        today = timezone.now().date()
        period_days = 10
        start, end = today - timedelta(days=period_days), today
        default_opp_data = dict(start=start, end=end, brand="Test")
        opp_data = {**default_opp_data, **(opp_data or dict())}
        camp_data = camp_data or dict(name="Campaign name")
        pl_data = pl_data or dict(ordered_rate=0.6)
        opportunity = Opportunity.objects.create(id="opportunity_" + str(_id),
                                                 name="",
                                                 **opp_data)
        placement = OpPlacement.objects.create(
            id="op_placement_" + str(_id), name="", opportunity=opportunity,
            goal_type_id=goal_type,
            **pl_data)

        campaign = Campaign.objects.create(
            id=int(_id),
            salesforce_placement=placement, **camp_data
        )
        if generate_statistic:
            generate_campaign_statistic(campaign, opp_data["start"],
                                        opp_data["end"])
        return opportunity, campaign


def generate_campaign_statistic(
        campaign, start, end, predefined_statistics=None):
    for i in range((end - start).days + 1):
        base_stats = {
            "campaign": campaign,
            "date": start + timedelta(days=i),
            "impressions": 10,
            "video_views": 4,
            "cost": 2
        }
        if predefined_statistics is not None:
            base_stats.update(predefined_statistics)
        CampaignStatistic.objects.create(**base_stats)
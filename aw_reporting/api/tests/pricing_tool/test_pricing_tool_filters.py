from datetime import date

from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK

from aw_reporting.api.urls.names import Name
from aw_reporting.models import AdGroup, Opportunity, OpPlacement, Account, \
    Campaign, Audience, AudienceStatistic
from saas.urls.namespaces import Namespace
from userprofile.models import UserSettingsKey
from utils.utils_tests import ExtendedAPITestCase


class PricingToolTestCase(ExtendedAPITestCase):
    url = reverse(Namespace.AW_REPORTING + ":" + Name.PricingTool.FILTERS)

    def setUp(self):
        self.user = self.create_test_user()

    def test_pricing_tool_filters(self):
        url = reverse("aw_reporting_urls:pricing_tool_filters")
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(
            set(response.data.keys()),
            {
                "quarters", "quarters_condition",
                "start", "end",
                "compare_yoy",

                "product_types", "product_types_condition",

                "parents", "genders", "ages",
                "demographic_condition",

                "creative_lengths", "creative_lengths_condition",
                "targeting_types", "targeting_types_condition",
                "geo_locations", "geo_locations_condition",

                "topics", "topics_condition",
                "interests", "interests_condition",
                "brands", "categories",

                "devices", "devices_condition",

                "exclude_campaigns", "exclude_opportunities",

                "ctr", "ctr_v", "video_view_rate", "video100rate"
            }
        )

    def test_filters_hides_data_from_not_visible_accounts(self):
        opportunity_visible = Opportunity.objects.create(
            id="1", brand="brand 1", category_id="category 1")
        opportunity_hidden = Opportunity.objects.create(
            id="2", brand="brand 2", category_id="category 2")
        placement_visible = OpPlacement.objects.create(
            id="1", opportunity=opportunity_visible)
        placement_hidden = OpPlacement.objects.create(
            id="2", opportunity=opportunity_hidden)
        account_visible = Account.objects.create(id="1")
        account_hidden = Account.objects.create(id="2")
        campaign_visible = Campaign.objects.create(
            id="1", salesforce_placement=placement_visible,
            account=account_visible)
        campaign_hidden = Campaign.objects.create(
            id="2", salesforce_placement=placement_hidden,
            account=account_hidden)
        ad_group_visible = AdGroup.objects.create(
            id="1", campaign=campaign_visible, type="type1")
        # hidden
        AdGroup.objects.create(
            id="2", campaign=campaign_hidden, type="type2")

        user_settings = {
            UserSettingsKey.GLOBAL_ACCOUNT_VISIBILITY: True,
            UserSettingsKey.VISIBLE_ACCOUNTS: [account_visible.id]
        }
        with self.patch_user_settings(**user_settings):
            response = self.client.get(self.url)

        self.assertEqual(response.status_code, HTTP_200_OK)
        filters = response.data

        def id_name_list(i):
            return [dict(id=i, name=i)]

        self.assertEqual(filters["product_types"],
                         id_name_list(ad_group_visible.type))
        self.assertEqual(filters["brands"],
                         id_name_list(opportunity_visible.brand))
        self.assertEqual(filters["categories"],
                         id_name_list(opportunity_visible.category_id))

    def test_interests_contains_type(self):
        any_date = date(2018, 1, 1)
        opportunity = Opportunity.objects.create()
        placement = OpPlacement.objects.create(opportunity=opportunity)
        campaign = Campaign.objects.create(salesforce_placement=placement)
        ad_group = AdGroup.objects.create(campaign=campaign)
        a_1 = Audience.objects.create(id=1, name="A1",
                                      type=Audience.IN_MARKET_TYPE)
        a_2 = Audience.objects.create(id=2, name="A2",
                                      type=Audience.AFFINITY_TYPE)
        a_3 = Audience.objects.create(id=3, name="A3",
                                      type=Audience.CUSTOM_AFFINITY_TYPE)
        expected_interests = sorted(
            Audience.objects.all().values("id", "name", "type"),
            key=lambda i: i["name"])

        common = dict(ad_group=ad_group, date=any_date)
        AudienceStatistic.objects.create(audience=a_1, **common)
        AudienceStatistic.objects.create(audience=a_2, **common)
        AudienceStatistic.objects.create(audience=a_3, **common)

        resonse = self.client.get(self.url)

        self.assertEqual(resonse.status_code, HTTP_200_OK)
        self.assertEqual(resonse.data["interests"], expected_interests)

from datetime import date

from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK

from aw_reporting.api.urls.names import Name
from aw_reporting.models import AdGroup, Opportunity, OpPlacement, Account, \
    Campaign, Audience, AudienceStatistic
from saas.urls.namespaces import Namespace
from utils.utils_tests import ExtendedAPITestCase, patch_instance_settings


class PricingToolTestCase(ExtendedAPITestCase):
    url = reverse(Namespace.AW_REPORTING + ":" + Name.PricingTool.FILTERS)

    def setUp(self):
        self.user = self.create_test_user()

    def test_pricing_tool_filters(self):
        response = self.client.get(self.url)
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

                "interests_affinity", "interests_in_marketing",
                "interests_condition",

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

        with patch_instance_settings(visible_accounts=[account_visible.id]):
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

    def test_interests_grouped(self):
        any_date = date(2018, 1, 1)
        campaign = Campaign.objects.create()
        ad_group = AdGroup.objects.create(campaign=campaign)
        affinity_audience_1 = Audience.objects.create(
            id=1, name="Test 1 affinity", type=Audience.AFFINITY_TYPE)
        in_marketing_audience = Audience.objects.create(
            id=2, name="Test In-Marketing", type=Audience.IN_MARKET_TYPE)
        affinity_audience_2 = Audience.objects.create(
            id=3, name="Test 2 affinity", type=Audience.AFFINITY_TYPE)

        common = dict(date=any_date, ad_group=ad_group)
        AudienceStatistic.objects.create(audience=affinity_audience_1, **common)
        AudienceStatistic.objects.create(audience=affinity_audience_2, **common)
        AudienceStatistic.objects.create(audience=in_marketing_audience,
                                         **common)
        affinity_audiences = sorted(
            [
                dict(id=affinity_audience_1.id, name=affinity_audience_1.name),
                dict(id=affinity_audience_2.id, name=affinity_audience_2.name),
            ],
            key=lambda i: i["name"])

        response = self.client.get(self.url)

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["interests_affinity"],
                         affinity_audiences)
        self.assertEqual(response.data["interests_in_marketing"],
                         [dict(id=in_marketing_audience.id,
                               name=in_marketing_audience.name)])

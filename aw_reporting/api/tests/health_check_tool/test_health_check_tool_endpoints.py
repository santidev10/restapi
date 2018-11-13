from datetime import date
from datetime import timedelta

from django.contrib.auth import get_user_model
from django.http import QueryDict
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_401_UNAUTHORIZED

from aw_reporting.api.urls.names import Name
from aw_reporting.models import AdGroup
from aw_reporting.models import Campaign
from aw_reporting.models import CampaignAgeRangeTargeting
from aw_reporting.models import CampaignGenderTargeting
from aw_reporting.models import CampaignLocationTargeting
from aw_reporting.models import GeoTarget
from aw_reporting.models import OpPlacement
from aw_reporting.models import Opportunity
from aw_reporting.models import SFAccount
from aw_reporting.models import User
from aw_reporting.models import VideoCreative
from aw_reporting.models import VideoCreativeStatistic
from aw_reporting.tools.health_check_tool import AGE_RANGES
from aw_reporting.tools.health_check_tool import GENDERS
from saas.urls.namespaces import Namespace
from userprofile.models import UserSettingsKey
from utils.utittests.test_case import ExtendedAPITestCase as APITestCase
from utils.utittests.generic_test import generic_test
from utils.utittests.int_iterator import int_iterator
from utils.utittests.reverse import reverse


class AWSetupHealthCheckListTestCase(APITestCase):
    url = reverse(Name.HealthCheck.LIST, [Namespace.AW_REPORTING])

    def setUp(self):
        self.user = self.create_test_user()

    def test_fail_get_list(self):
        self.__create_not_auth_user()
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_401_UNAUTHORIZED)

    def test_get_list(self):
        user1 = User.objects.create(
            id="1", name="Daisy", email="1@mail.go", is_active=True)
        user2 = User.objects.create(
            id="2", name="Jay", email="2@mail.go", is_active=True)
        user3 = User.objects.create(
            id="3", name="Tom", email="3@mail.go", is_active=True)
        user4 = User.objects.create(
            id="4", name="Cally", email="4@mail.go", is_active=True)
        s_user1 = get_user_model().objects.create(
            email=user1.email, profile_image_url="1.png")
        s_user2 = get_user_model().objects.create(
            email=user2.email, profile_image_url="2.png")
        s_user3 = get_user_model().objects.create(
            email=user3.email, profile_image_url="3.png")
        s_user4 = get_user_model().objects.create(
            email=user4.email, profile_image_url="4.png")
        opportunity = Opportunity.objects.create(
            id="A", name="First campaign", probability=100,
            start="2017-01-20", end="2017-01-30", ad_ops_manager=user1,
            account_manager=user2, ad_ops_qa_manager=user3,
            sales_manager=user4,
            demographic="A 35 - 44;A 45 - 54;A 65+;No unknown age",
            targeting_tactics="3p Targeting;Custom Affinity;"
                              "Interest;Keyword;Topic",
            tags="Attached See Below",
            geo_targeting="""
                Only the following states:
                Washington, United States
                Utah, United States
            """,
            brand="test brand")
        placement = OpPlacement.objects.create(
            id="A", name="", opportunity=opportunity)
        campaign = Campaign.objects.create(
            id="1", name="", salesforce_placement=placement,
            start_date=opportunity.start, end_date=None,
            targeting_interests=True, targeting_topics=True,
            targeting_keywords=True, tracking_template_is_set=True)
        Campaign.objects.create(
            id="2", name="", salesforce_placement=placement,
            start_date=opportunity.start)
        CampaignAgeRangeTargeting.objects.create(
            campaign=campaign, age_range_id=AGE_RANGES.index("35-44"))
        CampaignAgeRangeTargeting.objects.create(
            campaign=campaign, age_range_id=AGE_RANGES.index("45-54"))
        CampaignAgeRangeTargeting.objects.create(
            campaign=campaign, age_range_id=AGE_RANGES.index("65 or more"))
        CampaignGenderTargeting.objects.create(
            campaign=campaign, gender_id=GENDERS.index("Male"))
        CampaignGenderTargeting.objects.create(
            campaign=campaign, gender_id=GENDERS.index("Female"))
        CampaignGenderTargeting.objects.create(
            campaign=campaign, gender_id=GENDERS.index("Undetermined Gender"))
        geo_target, _ = GeoTarget.objects.get_or_create(
            id=21180, defaults=dict(
                name="Washington", canonical_name="Washington, United States"))
        CampaignLocationTargeting.objects.create(
            campaign=campaign, location=geo_target)
        geo_target, _ = GeoTarget.objects.get_or_create(
            id=21177, defaults=dict(
                name="Utah", canonical_name="Utah, United States"))
        CampaignLocationTargeting.objects.create(
            campaign=campaign, location=geo_target)
        ad_group = AdGroup.objects.create(id="1", name="", campaign=campaign)
        creative = VideoCreative.objects.create(id="YYY")
        VideoCreativeStatistic.objects.create(
            creative=creative, ad_group=ad_group, date="2017-01-20")
        Opportunity.objects.create(
            id="B", name="Second campaign", probability=100,
            start="2016-01-20", end="2016-01-30")
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data["items"]), 2)
        item = response.data["items"][0]
        self.assertEqual(
            set(item.keys()),
            {"id", "name", "thumbnail", "qa", "am", "ad_ops", "sales",
             "flight", "targeting", "demographic", "geographic", "tags",
             "brand", "gdn"})
        self.assertEqual(item["id"], opportunity.id)
        self.assertEqual(item["name"], opportunity.name)
        self.assertEqual(item["thumbnail"],
                         "https://i.ytimg.com/vi/YYY/hqdefault.jpg")
        self.assertEqual(item["brand"], opportunity.brand)
        self.assertEqual(item["ad_ops"]["id"], user1.id)
        self.assertEqual(item["ad_ops"]["name"], user1.name)
        self.assertEqual(item["ad_ops"]["thumbnail"],
                         s_user1.profile_image_url)
        self.assertEqual(item["am"]["id"], user2.id)
        self.assertEqual(item["am"]["name"], user2.name)
        self.assertEqual(item["am"]["thumbnail"], s_user2.profile_image_url)
        self.assertEqual(item["qa"]["id"], user3.id)
        self.assertEqual(item["qa"]["name"], user3.name)
        self.assertEqual(item["qa"]["thumbnail"], s_user3.profile_image_url)
        self.assertEqual(item["sales"]["id"], user4.id)
        self.assertEqual(item["sales"]["name"], user4.name)
        self.assertEqual(item["sales"]["thumbnail"], s_user4.profile_image_url)

    def test_get_list_filtered_by_date_range(self):
        date_range_start = date(2017, 1, 1)
        date_range_end = date(2018, 1, 1)
        date_delta = timedelta(days=1)
        date_ranges = (
            # include
            (None, None),
            (None, date_range_end - date_delta),
            (date_range_start + date_delta, None),
            (date_range_start, date_range_end),
            (date_range_start + date_delta, date_range_end - date_delta),
            (date_range_end, None),
            (None, date_range_start),
            # exclude
            (date_range_start - date_delta, date_range_end + date_delta),
            (date_range_start - date_delta, date_range_end - date_delta),
            (date_range_start + date_delta, date_range_end + date_delta),
            (date_range_start - date_delta, date_range_start),
            (date_range_end, date_range_end + date_delta),
            (date_range_start - date_delta, None),
            (date_range_end + date_delta, None),
            (None, date_range_start - date_delta),
            (None, date_range_end + date_delta))
        relevant_count = 7
        opportunities = [
            Opportunity.objects.create(
                id=str(index), name="", probability=100, start=start, end=end)
            for index, (start, end) in enumerate(date_ranges)]
        include_opportunity_ids = {
            op.id for op in opportunities[:relevant_count]}
        query_params = QueryDict("", mutable=True)
        query_params.update(dict(start=date_range_start, end=date_range_end))
        url = "?".join([
            self.url,
            query_params.urlencode()])
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["items_count"], relevant_count)
        self.assertEqual({item["id"] for item in response.data["items"]},
                         include_opportunity_ids)

    def test_get_list_filtered_by_brand(self):
        brand_1 = "test brand 1"
        brand_2 = "test brand 2"
        brand_3 = "test brand 3"
        Opportunity.objects.create(
            id="1", name="", probability=100, brand=brand_1)
        Opportunity.objects.create(
            id="2", name="", probability=100, brand=brand_2)
        Opportunity.objects.create(
            id="3", name="", probability=100, brand=brand_3)
        query_params = "brands={}".format(",".join([brand_1, brand_2]))
        url = "{}?{}".format(self.url, query_params)
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data["items"]), 2)
        brands = [item["brand"] for item in response.data["items"]]
        self.assertEqual(set(brands), {brand_1, brand_2})

    def test_get_list_filtered_by_sales_rep(self):
        user_1 = User.objects.create(
            id="1", name="Daisy", email="1@mail.go", is_active=True)
        user_2 = User.objects.create(
            id="2", name="Jay", email="2@mail.go", is_active=True)
        user_3 = User.objects.create(
            id="3", name="Tom", email="3@mail.go", is_active=True)
        op_1 = Opportunity.objects.create(
            id="AAA", name="", probability=100, sales_manager=user_1)
        op_2 = Opportunity.objects.create(
            id="BBB", name="", probability=100, sales_manager=user_2)
        Opportunity.objects.create(
            id="CCC", name="", probability=100, sales_manager=user_3)
        query_params = "sales_rep={}".format(",".join([user_1.id, user_2.id]))
        url = "{}?{}".format(self.url, query_params)
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data["items"]), 2)
        opportunity_ids = [item["id"] for item in response.data["items"]]
        self.assertEqual(set(opportunity_ids), {op_1.id, op_2.id})

    def test_get_list_filtered_by_campaign_start(self):
        left_date_border = date(2017, 1, 1)
        right_date_border = date(2017, 1, 5)
        start_dates = (
            # include
            left_date_border,
            date(2017, 1, 3),
            right_date_border,
            # exclude
            date(2016, 12, 30),
            date(2017, 1, 8))
        expected_opportunities_count = 3
        opportunities = [Opportunity.objects.create(
            id=str(opp_id), name="", probability=100, start=start_date)
            for opp_id, start_date in enumerate(start_dates)]
        expected_opportunities_ids = {
            opportunity.id for opportunity
            in opportunities[:expected_opportunities_count]}
        query_params = QueryDict("", mutable=True)
        query_params.update(
            dict(campaign_start=left_date_border,
                 campaign_end=right_date_border))
        url = "?".join([
            self.url,
            query_params.urlencode()])
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(
            response.data["items_count"], expected_opportunities_count)
        self.assertEqual(
            expected_opportunities_ids,
            {obj["id"] for obj in response.data["items"]})

    def __create_not_auth_user(self):
        self.user.delete()
        self.create_test_user(False)

    @generic_test([
        (global_account_visibility, (global_account_visibility, count), dict())
        for global_account_visibility, count in ((True, 0), (False, 1))
    ])
    def test_global_account_visibility(self, global_account_visibility, expected_count):
        Opportunity.objects.create(id=next(int_iterator), probability=100)
        user_settings = {
            UserSettingsKey.GLOBAL_ACCOUNT_VISIBILITY: global_account_visibility,
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: False,
            UserSettingsKey.VISIBLE_ACCOUNTS: []
        }
        with self.patch_user_settings(**user_settings):
            response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["items_count"], expected_count)


class AWSetupHealthCheckFiltersTestCase(APITestCase):
    url = reverse(Name.HealthCheck.FILTERS, [Namespace.AW_REPORTING])

    def setUp(self):
        self.user = self.create_test_user()

    def __create_not_auth_user(self):
        self.user.delete()
        self.create_test_user(False)

    def test_fail_get_filters(self):
        self.__create_not_auth_user()
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_401_UNAUTHORIZED)

    def test_get_filters(self):
        user_1 = User.objects.create(id="1", name="Mike", is_active=True)
        user_2 = User.objects.create(id="2", name="Alien", is_active=True)
        user_3 = User.objects.create(id="3", name="Samuel", is_active=True)
        user_4 = User.objects.create(id="4", name="John", is_active=True)
        account = SFAccount.objects.create(id="AAA", name="Acc")
        Opportunity.objects.create(
            id="AAA", name="", account=account,
            account_manager=user_1, sales_manager=user_3)
        Opportunity.objects.create(
            id="BBB", name="", account_manager=user_2,
            ad_ops_manager=user_1, sales_manager=user_4)
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        expected_keys = {"period", "am", "account", "ad_ops", "date_range",
                         "sales_rep", "brands"}
        self.assertEqual(set(response.data.keys()), expected_keys)
        self.assertEqual(len(response.data["am"]), 2)
        self.assertEqual(len(response.data["ad_ops"]), 1)
        self.assertEqual(len(response.data["account"]), 1)
        self.assertEqual(len(response.data["sales_rep"]), 2)
        sales_ids = [sale["id"] for sale in response.data["sales_rep"]]
        self.assertEqual({user_3.id, user_4.id}, set(sales_ids))

    def test_get_filters_data_range(self):
        any_data = date(2017, 1, 1)
        op1_start = any_data
        op1_end = any_data + timedelta(days=10)
        op2_start = any_data + timedelta(days=15)
        op2_end = any_data + timedelta(days=20)
        Opportunity.objects.create(
            id="AAA", name="", start=op1_start, end=op1_end)
        Opportunity.objects.create(
            id="BBB", name="", start=op2_start, end=op2_end)
        Opportunity.objects.create(
            id="CCC", name="", start=None, end=None)
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertIn("date_range", set(response.data.keys()))
        self.assertEqual(
            response.data["date_range"], dict(min=op1_start, max=op2_end))

    def test_get_filters_data_range_is_empty_if_no_data(self):
        Opportunity.objects.create(id="CCC", name="", start=None, end=None)
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertIn("date_range", set(response.data.keys()))
        self.assertEqual(response.data["date_range"], dict(min=None, max=None))

    def test_get_filters_brands(self):
        brand_1 = "test brand 1"
        brand_2 = "test brand 2"
        Opportunity.objects.create(id="1", name="", brand=brand_1)
        Opportunity.objects.create(id="2", name="", brand=brand_2)
        Opportunity.objects.create(id="3", name="", brand=brand_1)
        Opportunity.objects.create(id="4", name="", brand=None)
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertIn("brands", set(response.data.keys()))
        brands = response.data["brands"]
        self.assertEqual(len(brands), 2)
        self.assertEqual(set(brands), {brand_1, brand_2})

    def test_get_filters_hide_not_active_media_service_manager(self):
        user_1 = User.objects.create(
            id="1", name="Daisy", email="1@mail.go", is_active=False)
        user_2 = User.objects.create(
            id="2", name="Jay", email="2@mail.go", is_active=True)
        Opportunity.objects.create(id="1", name="", ad_ops_manager=user_1)
        Opportunity.objects.create(id="2", name="", ad_ops_manager=user_2)
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertIn("ad_ops", set(response.data.keys()))
        self.assertEqual(len(response.data["ad_ops"]), 1)
        item = response.data["ad_ops"][0]
        self.assertEqual(item["id"], user_2.id)

    def test_get_filters_hide_not_active_account_manager(self):
        user_1 = User.objects.create(
            id="1", name="Daisy", email="1@mail.go", is_active=False)
        user_2 = User.objects.create(
            id="2", name="Jay", email="2@mail.go", is_active=True)
        Opportunity.objects.create(id="1", name="", account_manager=user_1)
        Opportunity.objects.create(id="2", name="", account_manager=user_2)
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertIn("am", set(response.data.keys()))
        self.assertEqual(len(response.data["am"]), 1)
        item = response.data["am"][0]
        self.assertEqual(item["id"], user_2.id)

    def test_get_filters_hide_not_active_sales_manager(self):
        user_1 = User.objects.create(
            id="1", name="Daisy", email="1@mail.go", is_active=False)
        user_2 = User.objects.create(
            id="2", name="Jay", email="2@mail.go", is_active=True)
        Opportunity.objects.create(id="1", name="", sales_manager=user_1)
        Opportunity.objects.create(id="2", name="", sales_manager=user_2)
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertIn("sales_rep", set(response.data.keys()))
        self.assertEqual(len(response.data["sales_rep"]), 1)
        item = response.data["sales_rep"][0]
        self.assertEqual(item["id"], user_2.id)

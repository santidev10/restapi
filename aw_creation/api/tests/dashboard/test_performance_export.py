import json
from datetime import date
from datetime import datetime
from datetime import timedelta
from itertools import cycle
from itertools import product

from django.test import override_settings
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_400_BAD_REQUEST
from rest_framework.status import HTTP_403_FORBIDDEN
from rest_framework.status import HTTP_404_NOT_FOUND

from aw_creation.api.urls.names import Name
from aw_creation.api.urls.namespace import Namespace
from aw_creation.api.views.dashboard.performance_export import METRIC_REPRESENTATION
from aw_creation.api.views.dashboard.performance_export import Metric
from aw_reporting.calculations.cost import get_client_cost
from aw_reporting.dashboard_charts import DateSegment
from aw_reporting.demo.models import DEMO_ACCOUNT_ID
from aw_reporting.demo.recreate_demo_data import recreate_demo_data
from aw_reporting.excel_reports.dashboard_performance_report import COLUMN_NAME
from aw_reporting.excel_reports.dashboard_performance_report import DashboardPerformanceReportColumn
from aw_reporting.excel_reports.dashboard_performance_report import TOO_MUCH_DATA_MESSAGE
from aw_reporting.models import Account
from aw_reporting.models import Ad
from aw_reporting.models import AdGroup
from aw_reporting.models import AdGroupStatistic
from aw_reporting.models import AdStatistic
from aw_reporting.models import AgeRangeStatistic
from aw_reporting.models import Audience
from aw_reporting.models import AudienceStatistic
from aw_reporting.models import Campaign
from aw_reporting.models import CampaignStatistic
from aw_reporting.models import CityStatistic
from aw_reporting.models import GenderStatistic
from aw_reporting.models import GeoTarget
from aw_reporting.models import KeywordStatistic
from aw_reporting.models import OpPlacement
from aw_reporting.models import Opportunity
from aw_reporting.models import RemarkList
from aw_reporting.models import RemarkStatistic
from aw_reporting.models import SalesForceGoalType
from aw_reporting.models import Topic
from aw_reporting.models import TopicStatistic
from aw_reporting.models import VideoCreative
from aw_reporting.models import VideoCreativeStatistic
from aw_reporting.models import YTChannelStatistic
from aw_reporting.models import YTVideoStatistic
from saas.urls.namespaces import Namespace as RootNamespace
from userprofile.constants import UserSettingsKey
from utils.datetime import get_quarter
from utils.lang import flatten
from utils.utittests.generic_test import generic_test
from utils.utittests.int_iterator import int_iterator
from utils.utittests.patch_now import patch_now
from utils.utittests.reverse import reverse
from utils.utittests.test_case import ExtendedAPITestCase
from utils.utittests.xlsx import get_sheet_from_response


class DashboardPerformanceExportAPITestCase(ExtendedAPITestCase):
    def _get_url(self, account_creation_id):
        return reverse(Name.Dashboard.PERFORMANCE_EXPORT, [RootNamespace.AW_CREATION, Namespace.DASHBOARD],
                       args=(account_creation_id,))

    def _request(self, account_creation_id, **kwargs):
        url = self._get_url(account_creation_id)
        return self.client.post(url, json.dumps(kwargs), content_type="application/json", )

    def _create_stats(self, account, statistic_date=None):
        campaign1 = Campaign.objects.create(id=1, name="#1", account=account, video_views=1)
        ad_group1 = AdGroup.objects.create(id=1, name="", campaign=campaign1)
        campaign2 = Campaign.objects.create(id=2, name="#2", account=account, video_views=1)
        ad_group2 = AdGroup.objects.create(id=2, name="", campaign=campaign2)
        statistic_date = statistic_date or (datetime.now().date() - timedelta(days=1))
        base_stats = dict(date=statistic_date, impressions=100, video_views=10, cost=1)
        topic, _ = Topic.objects.get_or_create(id=1, defaults=dict(name="boo"))
        audience, _ = Audience.objects.get_or_create(id=1,
                                                     defaults=dict(name="boo",
                                                                   type="A"))
        remark, _ = RemarkList.objects.get_or_create(name="test")
        creative, _ = VideoCreative.objects.get_or_create(id=1)
        city, _ = GeoTarget.objects.get_or_create(id=1, defaults=dict(name="Babruysk"))
        ad = Ad.objects.create(id=1, ad_group=ad_group1)
        CampaignStatistic.objects.create(campaign=campaign1, clicks_website=1, **base_stats)
        AdStatistic.objects.create(ad=ad, average_position=1, **base_stats)

        for ad_group in (ad_group1, ad_group2):
            stats = dict(ad_group=ad_group, **base_stats)
            AdGroupStatistic.objects.create(average_position=1, **stats)
            GenderStatistic.objects.create(**stats)
            AgeRangeStatistic.objects.create(**stats)
            TopicStatistic.objects.create(topic=topic, **stats)
            AudienceStatistic.objects.create(audience=audience, **stats)
            VideoCreativeStatistic.objects.create(creative=creative, **stats)
            YTChannelStatistic.objects.create(yt_id="123", **stats)
            YTVideoStatistic.objects.create(yt_id="123", **stats)
            KeywordStatistic.objects.create(keyword="123", **stats)
            CityStatistic.objects.create(city=city, **stats)
            RemarkStatistic.objects.create(remark=remark, **stats)

    def test_success_for_chf_dashboard(self):
        user = self.create_test_user()
        user.add_custom_user_permission("view_dashboard")
        account = Account.objects.create(id=1, name="")
        self._create_stats(account)
        user_settings = {
            UserSettingsKey.VISIBLE_ACCOUNTS: [1],
        }
        with self.patch_user_settings(**user_settings):
            response = self._request(account.account_creation.id)
            self.assertEqual(response.status_code, HTTP_200_OK)

    def test_no_demo_data(self):
        user = self.create_test_user()
        user.add_custom_user_permission("view_dashboard")
        account = Account.objects.create(id=1, name="")

        campaign_name = "Test campaign"
        Campaign.objects.create(name=campaign_name)
        user_settings = {
            UserSettingsKey.VISIBLE_ACCOUNTS: [1],
        }
        with self.patch_user_settings(**user_settings):
            response = self._request(account.account_creation.id)
        self.assertEqual(response.status_code, HTTP_200_OK)
        sheet = get_sheet_from_response(response)
        self.assertTrue(is_empty_report(sheet))

    def test_visible_accounts(self):
        user = self.create_test_user()
        user.add_custom_user_permission("view_dashboard")
        account = Account.objects.create(id=next(int_iterator), name="")
        user_settings = {
            UserSettingsKey.VISIBLE_ACCOUNTS: [],
        }
        with self.patch_user_settings(**user_settings):
            response = self._request(account.account_creation.id)
        self.assertEqual(response.status_code, HTTP_404_NOT_FOUND)

    def test_sf_data_in_summary_row(self):
        user = self.create_test_user()
        any_date = date(2018, 1, 1)
        user.add_custom_user_permission("view_dashboard")
        opportunity = Opportunity.objects.create()
        placement = OpPlacement.objects.create(opportunity=opportunity, ordered_rate=.2, total_cost=23,
                                               goal_type_id=SalesForceGoalType.CPM)

        account = Account.objects.create(id=next(int_iterator), name="")
        campaign = Campaign.objects.create(name="", account=account, salesforce_placement=placement)
        ad_group = AdGroup.objects.create(campaign=campaign)
        impressions, views, aw_cost = 1234, 234, 12
        AdGroupStatistic.objects.create(date=any_date, ad_group=ad_group, average_position=1,
                                        cost=aw_cost, impressions=impressions, video_views=views)
        client_cost = get_client_cost(
            goal_type_id=placement.goal_type_id,
            dynamic_placement=placement.dynamic_placement,
            placement_type=placement.placement_type,
            ordered_rate=placement.ordered_rate,
            impressions=impressions,
            video_views=views,
            aw_cost=aw_cost,
            total_cost=placement.total_cost,
            tech_fee=placement.tech_fee,
            start=any_date,
            end=any_date
        )
        self.assertGreater(client_cost, 0)
        average_cpm = client_cost / impressions * 1000
        average_cpv = client_cost / views
        user_settings = {
            UserSettingsKey.VISIBLE_ACCOUNTS: [account.id],
            UserSettingsKey.DASHBOARD_AD_WORDS_RATES: False
        }
        with self.patch_user_settings(**user_settings):
            response = self._request(account.account_creation.id)
        self.assertEqual(response.status_code, HTTP_200_OK)
        sheet = get_sheet_from_response(response)
        self.assertFalse(is_empty_report(sheet))
        headers = tuple(cell.value for cell in sheet[HEADER_ROW_INDEX])
        cost_index = get_column_index(headers, DashboardPerformanceReportColumn.COST)
        cpm_index = get_column_index(headers, DashboardPerformanceReportColumn.AVERAGE_CPM)
        cpv_index = get_column_index(headers, DashboardPerformanceReportColumn.AVERAGE_CPV)
        self.assertAlmostEqual(sheet[SUMMARY_ROW_INDEX][cost_index].value, client_cost)
        self.assertAlmostEqual(sheet[SUMMARY_ROW_INDEX + 1][cost_index].value, client_cost)
        self.assertAlmostEqual(sheet[SUMMARY_ROW_INDEX][cpm_index].value, average_cpm)
        self.assertAlmostEqual(sheet[SUMMARY_ROW_INDEX][cpv_index].value, average_cpv)

    def test_hide_costs(self):
        user = self.create_test_user()
        any_date = date(2018, 1, 1)
        user.add_custom_user_permission("view_dashboard")
        opportunity = Opportunity.objects.create()
        placement = OpPlacement.objects.create(opportunity=opportunity, ordered_rate=.2, total_cost=23,
                                               goal_type_id=SalesForceGoalType.CPM)

        account = Account.objects.create(id=next(int_iterator), name="")
        campaign = Campaign.objects.create(name="", account=account, salesforce_placement=placement)
        ad_group = AdGroup.objects.create(campaign=campaign)
        AdGroupStatistic.objects.create(date=any_date, ad_group=ad_group, average_position=1,
                                        cost=1, impressions=1, video_views=1)
        user_settings = {
            UserSettingsKey.VISIBLE_ACCOUNTS: [account.id],
            UserSettingsKey.DASHBOARD_COSTS_ARE_HIDDEN: True
        }
        with self.patch_user_settings(**user_settings):
            response = self._request(account.account_creation.id)
        self.assertEqual(response.status_code, HTTP_200_OK)
        sheet = get_sheet_from_response(response)
        self.assertFalse(is_empty_report(sheet))
        headers = tuple(cell.value for cell in sheet[HEADER_ROW_INDEX])
        expected_headers = (
            None, "Name", "Impressions", "Views", "Clicks", "Call-to-Action overlay", "Website", "App Store", "Cards",
            "End cap", "Ctr(i)", "Ctr(v)", "View rate", "25%", "50%", "75%", "100%")

        self.assertEqual(headers, expected_headers)
        row_lengths = [len(row) for row in sheet.rows]
        self.assertTrue(all([length == len(expected_headers) for length in row_lengths]))

    def test_all_conversions_column_is_present(self):
        user = self.create_test_user()
        user.add_custom_user_permission("view_dashboard")
        any_date = date(2018, 1, 1)
        opportunity = Opportunity.objects.create()
        placement = OpPlacement.objects.create(
            opportunity=opportunity, ordered_rate=.2, total_cost=23, goal_type_id=SalesForceGoalType.CPM)
        account = Account.objects.create(id=next(int_iterator), name="")
        campaign = Campaign.objects.create(name="", account=account, salesforce_placement=placement)
        ad_group = AdGroup.objects.create(campaign=campaign)
        AdGroupStatistic.objects.create(
            date=any_date, ad_group=ad_group, average_position=1, cost=1, impressions=1, video_views=1)
        user_settings = {
            UserSettingsKey.VISIBLE_ACCOUNTS: [account.id],
            UserSettingsKey.SHOW_CONVERSIONS: True
        }
        with self.patch_user_settings(**user_settings):
            response = self._request(account.account_creation.id)
        self.assertEqual(response.status_code, HTTP_200_OK)
        sheet = get_sheet_from_response(response)
        self.assertFalse(is_empty_report(sheet))
        headers = tuple(cell.value for cell in sheet[HEADER_ROW_INDEX])
        expected_headers = (
            None, "Name", "Impressions", "Views", "Cost", "Average cpm", "Average cpv", "Clicks",
            "Call-to-Action overlay", "Website", "App Store", "Cards", "End cap", "Ctr(i)", "Ctr(v)", "View rate",
            "25%", "50%", "75%", "100%", "All conversions")
        self.assertEqual(headers, expected_headers)
        row_lengths = [len(row) for row in sheet.rows]
        self.assertTrue(all([length == len(expected_headers) for length in row_lengths]))

    def test_show_real_costs(self):
        user = self.create_test_user()
        any_date = date(2018, 1, 1)
        user.add_custom_user_permission("view_dashboard")
        opportunity = Opportunity.objects.create()
        placement = OpPlacement.objects.create(opportunity=opportunity, ordered_rate=.2, total_cost=23,
                                               goal_type_id=SalesForceGoalType.CPM)

        account = Account.objects.create(id=next(int_iterator), name="")
        campaign = Campaign.objects.create(name="", account=account, salesforce_placement=placement)
        ad_group = AdGroup.objects.create(campaign=campaign)
        aw_cost = 123
        AdGroupStatistic.objects.create(date=any_date, ad_group=ad_group, average_position=1,
                                        cost=aw_cost, impressions=1, video_views=1)
        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
            UserSettingsKey.DASHBOARD_AD_WORDS_RATES: True,
        }
        with self.patch_user_settings(**user_settings):
            response = self._request(account.account_creation.id)
        self.assertEqual(response.status_code, HTTP_200_OK)
        sheet = get_sheet_from_response(response)
        self.assertFalse(is_empty_report(sheet))
        headers = tuple(cell.value for cell in sheet[HEADER_ROW_INDEX])
        cost_column_index = headers.index("Cost")
        single_data_row_index = 3
        self.assertEqual(sheet[single_data_row_index][cost_column_index].value, aw_cost)

    def test_hide_real_costs(self):
        user = self.create_test_user()
        any_date = date(2018, 1, 1)
        user.add_custom_user_permission("view_dashboard")
        opportunity = Opportunity.objects.create()
        placement = OpPlacement.objects.create(opportunity=opportunity, ordered_rate=.2, total_cost=23,
                                               goal_type_id=SalesForceGoalType.CPM)

        account = Account.objects.create(id=next(int_iterator), name="")
        campaign = Campaign.objects.create(name="", account=account, salesforce_placement=placement)
        ad_group = AdGroup.objects.create(campaign=campaign)
        stats = AdGroupStatistic.objects.create(date=any_date, ad_group=ad_group, average_position=1,
                                                cost=123, impressions=1, video_views=1)
        client_cost = get_client_cost(
            goal_type_id=placement.goal_type_id,
            dynamic_placement=placement.dynamic_placement,
            placement_type=placement.placement_type,
            ordered_rate=placement.ordered_rate,
            impressions=stats.impressions,
            video_views=stats.video_views,
            aw_cost=stats.cost,
            total_cost=placement.total_cost,
            tech_fee=placement.tech_fee,
            start=any_date,
            end=any_date,
        )
        self.assertGreater(client_cost, 0)
        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
            UserSettingsKey.DASHBOARD_AD_WORDS_RATES: False,
        }
        with self.patch_user_settings(**user_settings):
            response = self._request(account.account_creation.id)
        self.assertEqual(response.status_code, HTTP_200_OK)
        sheet = get_sheet_from_response(response)
        self.assertFalse(is_empty_report(sheet))
        headers = tuple(cell.value for cell in sheet[HEADER_ROW_INDEX])
        cost_column_index = headers.index("Cost")
        single_data_row_index = 3
        self.assertEqual(sheet[single_data_row_index][cost_column_index].value, client_cost)

    @generic_test([
        (metric, (metric,), dict())
        for metric in Metric
    ])
    def test_metric(self, metric):
        user = self.create_test_user()
        user.add_custom_user_permission("view_dashboard")

        account = Account.objects.create(id=next(int_iterator), name="")
        self._create_stats(account)
        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
            UserSettingsKey.DASHBOARD_COSTS_ARE_HIDDEN: True,
            UserSettingsKey.DASHBOARD_CAMPAIGNS_SEGMENTED: True,
        }
        with self.patch_user_settings(**user_settings):
            response = self._request(account.account_creation.id, metric=metric.value)
        self.assertEqual(response.status_code, HTTP_200_OK)
        sheet = get_sheet_from_response(response)
        self.assertFalse(is_empty_report(sheet))
        data_rows = list(sheet.rows)[SUMMARY_ROW_INDEX:]
        self.assertEqual(len(data_rows), 1)
        self.assertEqual(data_rows[0][0].value, METRIC_REPRESENTATION[metric])

    def test_bad_request_on_wrong_metric(self):
        user = self.create_test_user()
        user.add_custom_user_permission("view_dashboard")

        account = Account.objects.create(id=next(int_iterator), name="")
        self._create_stats(account)
        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
            UserSettingsKey.DASHBOARD_COSTS_ARE_HIDDEN: True
        }
        with self.patch_user_settings(**user_settings):
            response = self._request(account.account_creation.id, metric="invalid_metric")
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_date_segment_invalid(self):
        user = self.create_test_user()
        user.add_custom_user_permission("view_dashboard")
        test_segment = "invalid"
        self.assertFalse(DateSegment.has_value(test_segment))
        account = Account.objects.create(id=next(int_iterator), name="")
        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
        }
        with self.patch_user_settings(**user_settings):
            response = self._request(account.account_creation.id, date_segment=test_segment)
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_date_segment_day(self):
        user = self.create_test_user()
        user.add_custom_user_permission("view_dashboard")
        account = Account.objects.create(id=next(int_iterator), name="")
        test_date = datetime(2018, 4, 5, 6, 7, 8, 9)
        self._create_stats(account, test_date)
        expected_date_label = test_date.strftime("%m/%d/%Y")
        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
        }
        with self.patch_user_settings(**user_settings):
            response = self._request(account.account_creation.id, date_segment=DateSegment.DAY.value)
        self.assertEqual(response.status_code, HTTP_200_OK)
        sheet = get_sheet_from_response(response)
        self.assertFalse(is_empty_report(sheet))

        headers = tuple(cell.value for cell in sheet[HEADER_ROW_INDEX])

        self.assertEqual(headers[1], "Date")
        data_rows = list(sheet.rows)[SUMMARY_ROW_INDEX:]
        self.assertEqual(data_rows[0][1].value, expected_date_label)

    @generic_test([
        (dt.strftime("%A"), (dt,), dict())
        for dt in (date(2018, 4, 5) + timedelta(days=offset) for offset in range(7))
    ])
    def test_date_segment_week(self, test_date):
        user = self.create_test_user()
        user.add_custom_user_permission("view_dashboard")
        account = Account.objects.create(id=next(int_iterator), name="")
        fake_start_of_the_week = test_date - timedelta(days=test_date.isoweekday() % 7)
        self.assertEqual(fake_start_of_the_week.strftime("%A"), "Sunday")
        self._create_stats(account, test_date)
        expected_date_label = fake_start_of_the_week.strftime("%m/%d/%Y (W%U)")
        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
        }
        with self.patch_user_settings(**user_settings):
            response = self._request(account.account_creation.id, date_segment=DateSegment.WEEK.value)
        self.assertEqual(response.status_code, HTTP_200_OK)
        sheet = get_sheet_from_response(response)
        self.assertFalse(is_empty_report(sheet))

        headers = tuple(cell.value for cell in sheet[HEADER_ROW_INDEX])

        self.assertEqual(headers[1], "Date")
        data_rows = list(sheet.rows)[SUMMARY_ROW_INDEX:]
        self.assertEqual(data_rows[0][1].value, expected_date_label)

    def test_date_segment_month(self):
        user = self.create_test_user()
        user.add_custom_user_permission("view_dashboard")
        account = Account.objects.create(id=next(int_iterator), name="")
        test_date = datetime(2018, 4, 5, 6, 7, 8, 9)
        self._create_stats(account, test_date)
        expected_date_label = test_date.strftime("%b-%y")
        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
        }
        with self.patch_user_settings(**user_settings):
            response = self._request(account.account_creation.id, date_segment=DateSegment.MONTH.value)
        self.assertEqual(response.status_code, HTTP_200_OK)
        sheet = get_sheet_from_response(response)
        self.assertFalse(is_empty_report(sheet))

        headers = tuple(cell.value for cell in sheet[HEADER_ROW_INDEX])

        self.assertEqual(headers[1], "Date")
        data_rows = list(sheet.rows)[SUMMARY_ROW_INDEX:]
        self.assertEqual(data_rows[0][1].value, expected_date_label)

    @generic_test([
        ("Q{}".format(quarter_number), (quarter_number,), dict())
        for quarter_number in range(1, 5)
    ])
    def test_date_segment_quarter(self, quarter_number):
        user = self.create_test_user()
        user.add_custom_user_permission("view_dashboard")
        account = Account.objects.create(id=next(int_iterator), name="")
        test_date_1 = datetime(2018, quarter_number * 3 - 1, 5, 6, 7, 8, 9)
        expected_date_label = "{year} Q{quarter_number}".format(
            quarter_number=quarter_number,
            year=test_date_1.year,
        )
        self._create_stats(account, test_date_1)

        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
        }
        with self.patch_user_settings(**user_settings):
            response = self._request(account.account_creation.id, date_segment=DateSegment.QUARTER.value)
        self.assertEqual(response.status_code, HTTP_200_OK)
        sheet = get_sheet_from_response(response)
        self.assertFalse(is_empty_report(sheet))

        headers = tuple(cell.value for cell in sheet[HEADER_ROW_INDEX])

        self.assertEqual(headers[1], "Date")
        data_rows = list(sheet.rows)[SUMMARY_ROW_INDEX:]
        self.assertEqual(data_rows[0][1].value, expected_date_label)

    def test_date_segment_grouped_by_quarter(self):
        user = self.create_test_user()
        user.add_custom_user_permission("view_dashboard")
        account = Account.objects.create(id=next(int_iterator), name="")
        campaign = Campaign.objects.create(account=account)
        ad_group = AdGroup.objects.create(campaign=campaign)
        test_date_1 = datetime(2018, 5, 31, 6, 7, 8, 9)
        test_date_2 = test_date_1 + timedelta(days=7)
        self.assertNotEqual(test_date_1.day, test_date_2.day)
        self.assertNotEqual(test_date_1.isocalendar()[1], test_date_2.isocalendar()[1])
        self.assertNotEqual(test_date_1.month, test_date_2.month)
        self.assertNotEqual(get_quarter(test_date_1), get_quarter(test_date_2))
        impressions = (2, 3)
        common = dict(
            average_position=1,
            ad_group=ad_group,
        )
        AdGroupStatistic.objects.create(date=test_date_1, impressions=impressions[0], **common)
        AdGroupStatistic.objects.create(date=test_date_2, impressions=impressions[1], **common)
        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
        }
        with self.patch_user_settings(**user_settings):
            response = self._request(account.account_creation.id, date_segment=DateSegment.QUARTER.value)
        self.assertEqual(response.status_code, HTTP_200_OK)
        sheet = get_sheet_from_response(response)
        self.assertFalse(is_empty_report(sheet))
        headers = tuple(cell.value for cell in sheet[HEADER_ROW_INDEX])
        impressions_column = get_column_index(headers, DashboardPerformanceReportColumn.IMPRESSIONS)
        data_rows = list(sheet.rows)[SUMMARY_ROW_INDEX:]
        self.assertEqual(len(data_rows), 2)
        self.assertEqual(data_rows[0][impressions_column].value, sum(impressions))
        self.assertEqual(data_rows[1][impressions_column].value, sum(impressions))

    def test_date_segment_year(self):
        user = self.create_test_user()
        user.add_custom_user_permission("view_dashboard")
        account = Account.objects.create(id=next(int_iterator), name="")
        test_date = datetime(2018, 4, 5, 6, 7, 8, 9)
        self._create_stats(account, test_date)
        expected_date_label = test_date.strftime("%Y")
        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
        }
        with self.patch_user_settings(**user_settings):
            response = self._request(account.account_creation.id, date_segment=DateSegment.YEAR.value)
        self.assertEqual(response.status_code, HTTP_200_OK)
        sheet = get_sheet_from_response(response)
        self.assertFalse(is_empty_report(sheet))

        headers = tuple(cell.value for cell in sheet[HEADER_ROW_INDEX])

        self.assertEqual(headers[1], "Date")
        data_rows = list(sheet.rows)[SUMMARY_ROW_INDEX:]
        self.assertEqual(data_rows[0][1].value, expected_date_label)

    def test_date_segment_split_by_day(self):
        user = self.create_test_user()
        user.add_custom_user_permission("view_dashboard")
        account = Account.objects.create(id=next(int_iterator), name="")
        campaign = Campaign.objects.create(account=account)
        ad_group = AdGroup.objects.create(campaign=campaign)
        test_date_1 = datetime(2018, 4, 5, 6, 7, 8, 9)
        test_date_2 = test_date_1 + timedelta(days=1)
        impressions = (2, 3)
        common = dict(
            average_position=1,
            ad_group=ad_group,
        )
        AdGroupStatistic.objects.create(date=test_date_1, impressions=impressions[0], **common)
        AdGroupStatistic.objects.create(date=test_date_2, impressions=impressions[1], **common)
        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
        }
        with self.patch_user_settings(**user_settings):
            response = self._request(account.account_creation.id, date_segment=DateSegment.DAY.value)
        self.assertEqual(response.status_code, HTTP_200_OK)
        sheet = get_sheet_from_response(response)
        self.assertFalse(is_empty_report(sheet))
        headers = tuple(cell.value for cell in sheet[HEADER_ROW_INDEX])
        impressions_column = get_column_index(headers, DashboardPerformanceReportColumn.IMPRESSIONS)
        data_rows = list(sheet.rows)[SUMMARY_ROW_INDEX:]

        self.assertEqual(data_rows[0][impressions_column].value, impressions[0])
        self.assertEqual(data_rows[1][impressions_column].value, impressions[1])

    def test_date_segment_empty_in_summary(self):
        user = self.create_test_user()
        user.add_custom_user_permission("view_dashboard")
        account = Account.objects.create(id=next(int_iterator), name="")
        self._create_stats(account)
        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
        }
        with self.patch_user_settings(**user_settings):
            response = self._request(account.account_creation.id, date_segment=DateSegment.DAY.value)
        self.assertEqual(response.status_code, HTTP_200_OK)
        sheet = get_sheet_from_response(response)
        headers = tuple(cell.value for cell in sheet[HEADER_ROW_INDEX])
        date_column = get_column_index(headers, DashboardPerformanceReportColumn.DATE_SEGMENT)
        summary_row = sheet[SUMMARY_ROW_INDEX]
        self.assertIsNone(summary_row[date_column].value)

    def test_header_metric(self):
        user = self.create_test_user()
        test_metric = Metric.AD_GROUP
        test_metric_repr = METRIC_REPRESENTATION[test_metric]
        test_metric_value = test_metric.value
        user.add_custom_user_permission("view_dashboard")
        account = Account.objects.create(id=next(int_iterator), name="")
        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
        }
        with self.patch_user_settings(**user_settings):
            response = self._request(account.account_creation.id, metric=test_metric_value)
        sheet = get_sheet_from_response(response)
        header = get_custom_header(sheet)
        self.assertIn("Group By: {}".format(test_metric_repr), header)

    def test_header_date(self):
        user = self.create_test_user()
        user.add_custom_user_permission("view_dashboard")
        account = Account.objects.create(id=next(int_iterator), name="")
        self._create_stats(account)
        statistic_dates = CampaignStatistic.objects.all().values_list("date", flat=True)
        start = min(statistic_dates)
        end = max(statistic_dates)

        expected_date = "Date: {start} - {end}".format(start=start, end=end)
        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
            UserSettingsKey.DASHBOARD_CAMPAIGNS_SEGMENTED: True,
        }
        with self.patch_user_settings(**user_settings):
            response = self._request(account.account_creation.id)
        sheet = get_sheet_from_response(response)
        filters_header = get_custom_header(sheet)
        self.assertIn(expected_date, filters_header)

    def test_header_campaigns(self):
        user = self.create_test_user()
        user.add_custom_user_permission("view_dashboard")
        account = Account.objects.create(id=next(int_iterator), name="")
        campaigns = [
            Campaign.objects.create(id=next(int_iterator), name="test name 1", account=account),
            Campaign.objects.create(id=next(int_iterator), name="test name 2", account=account),
        ]
        ad_groups = [
            AdGroup.objects.create(id=next(int_iterator), campaign=campaign)
            for campaign in campaigns
        ]
        for ad_group in ad_groups:
            AdGroupStatistic.objects.create(date="2018-01-01", ad_group=ad_group, average_position=1, impressions=1)
        campaign_names = [campaign.name for campaign in campaigns]
        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
            UserSettingsKey.DASHBOARD_CAMPAIGNS_SEGMENTED: True,
        }
        with self.patch_user_settings(**user_settings):
            response = self._request(account.account_creation.id)
        sheet = get_sheet_from_response(response)
        header = get_custom_header(sheet)
        self.assertIn("Campaigns: {}".format(", ".join(campaign_names)), header)

    def test_header_ad_groups(self):
        user = self.create_test_user()
        user.add_custom_user_permission("view_dashboard")
        account = Account.objects.create(id=next(int_iterator), name="")
        campaign = Campaign.objects.create(id=next(int_iterator), account=account)
        ad_groups = [
            AdGroup.objects.create(id=next(int_iterator), name="test name 1", campaign=campaign),
            AdGroup.objects.create(id=next(int_iterator), name="test name 2", campaign=campaign),
        ]
        for ad_group in ad_groups:
            AdGroupStatistic.objects.create(date="2018-01-01", ad_group=ad_group, average_position=1, impressions=1)
        ad_group_names = [ad_group.name for ad_group in ad_groups]
        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
            UserSettingsKey.DASHBOARD_CAMPAIGNS_SEGMENTED: True,
        }
        with self.patch_user_settings(**user_settings):
            response = self._request(account.account_creation.id)
        sheet = get_sheet_from_response(response)
        header = get_custom_header(sheet)
        self.assertIn("Ad Groups: {}".format(", ".join(ad_group_names)), header)

    def test_filters_no_filters(self):
        user = self.create_test_user()
        user.add_custom_user_permission("view_dashboard")
        expected_header = "\n".join([
            "Date: None - None",
            "Group By: None",
            "Campaigns: ",
            "Ad Groups: ",
        ])
        account = Account.objects.create(id=next(int_iterator), name="")
        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
            UserSettingsKey.DASHBOARD_CAMPAIGNS_SEGMENTED: True,
        }
        with self.patch_user_settings(**user_settings):
            response = self._request(account.account_creation.id)
        sheet = get_sheet_from_response(response)
        header = get_custom_header(sheet)
        self.assertEqual(header, expected_header)

    def test_too_much_data(self):
        user = self.create_test_user()
        user.add_custom_user_permission("view_dashboard")
        test_data_limit = 1
        test_limit = SUMMARY_ROW_INDEX + test_data_limit
        account = Account.objects.create(id=next(int_iterator), name="")
        self._create_stats(account)
        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
        }
        with self.patch_user_settings(**user_settings), \
             override_settings(DASHBOARD_PERFORMANCE_REPORT_LIMIT=test_limit):
            response = self._request(account.account_creation.id)
        sheet = get_sheet_from_response(response)
        data_rows = list(sheet.rows)[SUMMARY_ROW_INDEX:]
        self.assertEqual(len(data_rows), test_data_limit + 1)
        self.assertEqual(data_rows[test_data_limit][0].value, TOO_MUCH_DATA_MESSAGE)

    def test_filename(self):
        user = self.create_test_user()
        test_now = datetime(2018, 6, 24)
        user.add_custom_user_permission("view_dashboard")
        test_account_name = "Test Account"
        account = Account.objects.create(id=next(int_iterator), name=test_account_name)
        expected_filename = "Segmented report {account_name} {year}{month}{day}.xlsx".format(
            account_name=test_account_name,
            year=test_now.year,
            month=("0" + str(test_now.month))[-2:],
            day=("0" + str(test_now.day))[-2:],
        )
        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
        }
        with self.patch_user_settings(**user_settings), \
             patch_now(test_now):
            response = self._request(account.account_creation.id)
        self.assertEqual(response["content-disposition"], "attachment; filename=\"{}\"".format(expected_filename))

    def test_hide_campaigns_from_header_if_it_shown_for_user(self):
        user = self.create_test_user()
        user.add_custom_user_permission("view_dashboard")
        expected_header = "\n".join([
            "Date: None - None",
            "Group By: None",
        ])
        account = Account.objects.create(id=next(int_iterator))
        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
            UserSettingsKey.DASHBOARD_CAMPAIGNS_SEGMENTED: False,
        }
        with self.patch_user_settings(**user_settings):
            response = self._request(account.account_creation.id)
        sheet = get_sheet_from_response(response)
        header = get_custom_header(sheet)
        self.assertEqual(header, expected_header)

    def test_hide_audience_based_on_settings(self):
        user = self.create_test_user()
        user.add_custom_user_permission("view_dashboard")
        account = Account.objects.create(id=next(int_iterator))
        self._create_stats(account)
        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
            UserSettingsKey.HIDE_REMARKETING: True,
        }
        with self.patch_user_settings(**user_settings):
            response = self._request(account.account_creation.id)
        sheet = get_sheet_from_response(response)
        metrics = {row[0].value for row in sheet}
        self.assertNotIn(METRIC_REPRESENTATION[Metric.AUDIENCE], metrics)

    def test_hide_campaigns_based_on_settings(self):
        user = self.create_test_user()
        user.add_custom_user_permission("view_dashboard")
        account = Account.objects.create(id=next(int_iterator))
        self._create_stats(account)
        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
            UserSettingsKey.DASHBOARD_CAMPAIGNS_SEGMENTED: False,
        }
        with self.patch_user_settings(**user_settings):
            response = self._request(account.account_creation.id)
        sheet = get_sheet_from_response(response)
        metrics = {row[0].value for row in sheet}
        self.assertNotIn(METRIC_REPRESENTATION[Metric.CAMPAIGN], metrics)

    @generic_test([
        (Metric.CAMPAIGN, (Metric.CAMPAIGN,), dict()),
        (Metric.AUDIENCE, (Metric.AUDIENCE,), dict()),
    ])
    def test_forbidden_if_user_has_no_permissions_for_the_metric(self, metric):
        user = self.create_test_user()
        user.add_custom_user_permission("view_dashboard")

        account = Account.objects.create(id=next(int_iterator), name="")
        self._create_stats(account)
        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
            UserSettingsKey.DASHBOARD_CAMPAIGNS_SEGMENTED: False,
            UserSettingsKey.HIDE_REMARKETING: True,
        }
        with self.patch_user_settings(**user_settings):
            response = self._request(account.account_creation.id, metric=metric.value)
        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)

    def test_campaign_no_valid_stats(self):
        user = self.create_test_user()
        user.add_custom_user_permission("view_dashboard")

        account = Account.objects.create(id=next(int_iterator), name="")
        campaign = Campaign.objects.create(id=next(int_iterator), account=account)
        impressions = 123
        CampaignStatistic.objects.create(campaign=campaign, date="2018-01-01", impressions=impressions)
        ad_groups = [
            AdGroup.objects.create(id=next(int_iterator), campaign=campaign)
            for _ in range(5)
        ]
        ad_group_ids = [ad_group.id for ad_group in ad_groups]
        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
            UserSettingsKey.DASHBOARD_CAMPAIGNS_SEGMENTED: True,
        }
        with self.patch_user_settings(**user_settings):
            response = self._request(account.account_creation.id,
                                     metric=Metric.CAMPAIGN.value,
                                     ad_groups=ad_group_ids)
        self.assertEqual(response.status_code, HTTP_200_OK)
        sheet = get_sheet_from_response(response)
        headers = tuple(cell.value for cell in sheet[HEADER_ROW_INDEX])
        impressions_index = get_column_index(headers, DashboardPerformanceReportColumn.IMPRESSIONS)
        self.assertEqual(sheet[SUMMARY_ROW_INDEX + 1][impressions_index].value, impressions)

    def test_success_for_demo_account(self):
        recreate_demo_data()
        user = self.create_test_user()
        user.add_custom_user_permission("view_dashboard")
        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
            UserSettingsKey.DASHBOARD_CAMPAIGNS_SEGMENTED: True,
        }
        with self.patch_user_settings(**user_settings):
            response = self._request(DEMO_ACCOUNT_ID)

        self.assertEqual(response.status_code, HTTP_200_OK)

    def test_demo_header(self):
        recreate_demo_data()
        user = self.create_test_user()
        user.add_custom_user_permission("view_dashboard")
        account = Account.objects.get(pk=DEMO_ACCOUNT_ID)
        campaigns = account.campaigns.all().order_by("name")
        statistic_dates = CampaignStatistic.objects.filter(campaign__in=campaigns).values_list("date", flat=True)
        start = min(statistic_dates)
        end = max(statistic_dates)
        ad_groups = AdGroup.objects.filter(campaign__in=campaigns).order_by("name")
        expected_header = "\n".join([
            "Date: {start} - {end}",
            "Group By: None",
            "Campaigns: {campaigns}",
            "Ad Groups: {ad_groups}",
        ]).format(
            start=start,
            end=end,
            campaigns=", ".join([campaign.name for campaign in campaigns]),
            ad_groups=", ".join([ad_group.name for ad_group in ad_groups]),
        )
        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
            UserSettingsKey.DASHBOARD_CAMPAIGNS_SEGMENTED: True,
        }
        with self.patch_user_settings(**user_settings):
            response = self._request(DEMO_ACCOUNT_ID)

        self.assertEqual(response.status_code, HTTP_200_OK)
        sheet = get_sheet_from_response(response)
        header = get_custom_header(sheet)
        self.assertEqual(header, expected_header)

    def test_metric_overview_grouped(self):
        user = self.create_test_user()
        user.add_custom_user_permission("view_dashboard")

        account = Account.objects.create(id=next(int_iterator), name="Test account")
        campaign_count = 3
        ad_group_count = 4
        campaigns = [Campaign.objects.create(id=next(int_iterator), account=account) for _ in range(campaign_count)]
        ad_groups = [AdGroup.objects.create(id=next(int_iterator), campaign=campaign)
                     for campaign, _ in product(campaigns, range(ad_group_count))]
        dates = [date(2018, 1, 1) + timedelta(days=i) for i in range(40)]
        impressions_generator = cycle([234, 124, 12436])
        expected_impressions = 0
        for ad_group, stats_date in product(ad_groups, dates):
            impressions = next(impressions_generator)
            expected_impressions += impressions

            AdGroupStatistic.objects.create(date=stats_date, ad_group=ad_group,
                                            average_position=1,
                                            impressions=impressions)

        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
        }
        with self.patch_user_settings(**user_settings):
            response = self._request(account.account_creation.id,
                                     metric=Metric.OVERVIEW.value)
        self.assertEqual(response.status_code, HTTP_200_OK)
        sheet = get_sheet_from_response(response)
        self.assertEqual(sheet.max_row, SUMMARY_ROW_INDEX + 1)
        headers = tuple(cell.value for cell in sheet[HEADER_ROW_INDEX])
        name_index = get_column_index(headers, DashboardPerformanceReportColumn.NAME)
        impressions_index = get_column_index(headers, DashboardPerformanceReportColumn.IMPRESSIONS)
        self.assertEqual(sheet[SUMMARY_ROW_INDEX + 1][name_index].value, account.name)
        self.assertGreater(expected_impressions, 0)
        self.assertEqual(sheet[SUMMARY_ROW_INDEX + 1][impressions_index].value, expected_impressions)

    def test_metric_overview_cta(self):
        user = self.create_test_user()
        user.add_custom_user_permission("view_dashboard")

        account = Account.objects.create(id=next(int_iterator), name="Test account")
        self._create_stats(account)
        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
        }
        with self.patch_user_settings(**user_settings):
            response = self._request(account.account_creation.id,
                                     metric=Metric.OVERVIEW.value)
        self.assertEqual(response.status_code, HTTP_200_OK)
        sheet = get_sheet_from_response(response)
        self.assertEqual(sheet.max_row, SUMMARY_ROW_INDEX + 1)
        headers = tuple(cell.value for cell in sheet[HEADER_ROW_INDEX])
        cta_index = get_column_index(headers, DashboardPerformanceReportColumn.CLICKS_CTA_WEBSITE)
        self.assertIsNotNone(sheet[SUMMARY_ROW_INDEX + 1][cta_index].value)

    def test_view_rate(self):
        user = self.create_test_user()
        user.add_custom_user_permission("view_dashboard")

        account = Account.objects.create(id=next(int_iterator))
        self._create_stats(account)
        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
        }
        with self.patch_user_settings(**user_settings):
            response = self._request(account.account_creation.id)
        self.assertEqual(response.status_code, HTTP_200_OK)
        sheet = get_sheet_from_response(response)
        headers = tuple(cell.value for cell in sheet[HEADER_ROW_INDEX])
        view_rate_index = get_column_index(headers, DashboardPerformanceReportColumn.VIEW_RATE)
        view_rate = float(sheet[SUMMARY_ROW_INDEX][view_rate_index].value)
        self.assertGreater(view_rate, 0)

    def test_demo_account_campaigns(self):
        recreate_demo_data()
        user = self.create_test_user()
        user.add_custom_user_permission("view_dashboard")
        campaigns = Campaign.objects.filter()
        expected_campaigns_names = set([campaign.name for campaign in campaigns])
        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
            UserSettingsKey.DASHBOARD_CAMPAIGNS_SEGMENTED: True,
        }
        with self.patch_user_settings(**user_settings):
            response = self._request(DEMO_ACCOUNT_ID, metric=Metric.CAMPAIGN.value)

        self.assertEqual(response.status_code, HTTP_200_OK)
        sheet = get_sheet_from_response(response)
        headers = tuple(cell.value for cell in sheet[HEADER_ROW_INDEX])
        name_index = get_column_index(headers, DashboardPerformanceReportColumn.NAME)
        campaigns_names = set([row[name_index].value for row in list(sheet.rows)[SUMMARY_ROW_INDEX:]])
        self.assertEqual(campaigns_names, expected_campaigns_names)

    def test_campaigns_cta(self):
        user = self.create_test_user()
        user.add_custom_user_permission("view_dashboard")

        account = Account.objects.create(id=next(int_iterator))
        self._create_stats(account)
        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
            UserSettingsKey.DASHBOARD_CAMPAIGNS_SEGMENTED: True,
        }
        with self.patch_user_settings(**user_settings):
            response = self._request(account.account_creation.id, metric=Metric.CAMPAIGN.value)
        self.assertEqual(response.status_code, HTTP_200_OK)
        sheet = get_sheet_from_response(response)
        headers = tuple(cell.value for cell in sheet[HEADER_ROW_INDEX])
        cta_website_index = get_column_index(headers, DashboardPerformanceReportColumn.CLICKS_CTA_WEBSITE)
        cta_website = [row[cta_website_index].value for row in list(sheet.rows)[SUMMARY_ROW_INDEX:]]
        self.assertGreater(len(cta_website), 0)
        self.assertFalse(any([cta is None for cta in cta_website]))
        self.assertTrue(any([cta > 0 for cta in cta_website]))

    def test_overview_is_always_visible(self):
        user = self.create_test_user()
        user.add_custom_user_permission("view_dashboard")

        account = Account.objects.create(id=next(int_iterator))
        self._create_stats(account)
        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
        }
        with self.patch_user_settings(**user_settings):
            response = self._request(account.account_creation.id)
        self.assertEqual(response.status_code, HTTP_200_OK)
        sheet = get_sheet_from_response(response)
        section_name_index = 0
        section_names = set([row[section_name_index].value for row in list(sheet.rows)[SUMMARY_ROW_INDEX:]])
        self.assertIn(METRIC_REPRESENTATION[Metric.OVERVIEW], section_names)

    def test_week_starts_on_sunday(self):
        user = self.create_test_user()
        user.add_custom_user_permission("view_dashboard")

        test_date_sunday = date(2018, 10, 28)
        test_date_monday = test_date_sunday + timedelta(days=1)
        impressions = 1, 2
        account = Account.objects.create(id=next(int_iterator))
        campaign = Campaign.objects.create(account=account)
        CampaignStatistic.objects.create(campaign=campaign, date=test_date_sunday, impressions=impressions[0])
        CampaignStatistic.objects.create(campaign=campaign, date=test_date_monday, impressions=impressions[1])

        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
            UserSettingsKey.DASHBOARD_CAMPAIGNS_SEGMENTED: True,
        }
        with self.patch_user_settings(**user_settings):
            response = self._request(
                account.account_creation.id,
                metric=Metric.CAMPAIGN.value,
                date_segment=DateSegment.WEEK.value
            )
        self.assertEqual(response.status_code, HTTP_200_OK)
        sheet = get_sheet_from_response(response)
        headers = get_headers(sheet)
        data_rows = list(sheet.rows)[SUMMARY_ROW_INDEX:]
        self.assertEqual(len(data_rows), 1)

        impressions_column_index = get_column_index(headers, DashboardPerformanceReportColumn.IMPRESSIONS)
        self.assertEqual(data_rows[0][impressions_column_index].value, sum(impressions))


def get_headers(sheet):
    return tuple(cell.value for cell in sheet[HEADER_ROW_INDEX])


CUSTOM_HEADER_ROW_INDEX = 1
HEADER_ROW_INDEX = 2
SUMMARY_ROW_INDEX = 3


def is_summary_empty(sheet):
    values_columns_indexes = range(2, 15)
    return all([sheet[SUMMARY_ROW_INDEX][column].value is None for column in values_columns_indexes])


def is_empty_report(sheet):
    min_rows_count = SUMMARY_ROW_INDEX
    return sheet.max_row <= min_rows_count and is_summary_empty(sheet)


def get_column_index(headers, column):
    return headers.index(COLUMN_NAME[column])


def get_custom_header(sheet):
    return sheet[CUSTOM_HEADER_ROW_INDEX][1].value

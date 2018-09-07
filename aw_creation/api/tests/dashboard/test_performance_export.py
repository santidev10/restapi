import io
import json
from datetime import date
from datetime import datetime
from datetime import timedelta
from unittest.mock import patch

from openpyxl import load_workbook
from rest_framework.status import HTTP_200_OK

from aw_creation.api.urls.names import Name
from aw_creation.api.urls.namespace import Namespace
from aw_reporting.calculations.cost import get_client_cost
from aw_reporting.excel_reports_dashboard import PerformanceReport
from aw_reporting.excel_reports_dashboard import PerformanceReportColumn
from aw_reporting.models import Account
from aw_reporting.models import Ad
from aw_reporting.models import AdGroup
from aw_reporting.models import AdGroupStatistic
from aw_reporting.models import AdStatistic
from aw_reporting.models import AgeRangeStatistic
from aw_reporting.models import Audience
from aw_reporting.models import AudienceStatistic
from aw_reporting.models import Campaign
from aw_reporting.models import CityStatistic
from aw_reporting.models import GenderStatistic
from aw_reporting.models import GeoTarget
from aw_reporting.models import KeywordStatistic
from aw_reporting.models import OpPlacement
from aw_reporting.models import Opportunity
from aw_reporting.models import SalesForceGoalType
from aw_reporting.models import Topic
from aw_reporting.models import TopicStatistic
from aw_reporting.models import VideoCreative
from aw_reporting.models import VideoCreativeStatistic
from aw_reporting.models import YTChannelStatistic
from aw_reporting.models import YTVideoStatistic
from saas.urls.namespaces import Namespace as RootNamespace
from userprofile.models import UserSettingsKey
from utils.utils_tests import ExtendedAPITestCase
from utils.utils_tests import SingleDatabaseApiConnectorPatcher
from utils.utils_tests import int_iterator
from utils.utils_tests import reverse


class DashboardPerformanceExportAPITestCase(ExtendedAPITestCase):
    def _get_url(self, account_creation_id):
        return reverse(Name.Dashboard.PERFORMANCE_EXPORT, [RootNamespace.AW_CREATION, Namespace.DASHBOARD],
                       args=(account_creation_id,))

    def _request(self, account_creation_id, **kwargs):
        url = self._get_url(account_creation_id)
        return self.client.post(url, json.dumps(kwargs), content_type="application/json", )

    def _create_stats(self, account):
        campaign1 = Campaign.objects.create(id=1, name="#1", account=account)
        ad_group1 = AdGroup.objects.create(id=1, name="", campaign=campaign1)
        campaign2 = Campaign.objects.create(id=2, name="#2", account=account)
        ad_group2 = AdGroup.objects.create(id=2, name="", campaign=campaign2)
        date = datetime.now().date() - timedelta(days=1)
        base_stats = dict(date=date, impressions=100, video_views=10, cost=1)
        topic, _ = Topic.objects.get_or_create(id=1, defaults=dict(name="boo"))
        audience, _ = Audience.objects.get_or_create(id=1,
                                                     defaults=dict(name="boo",
                                                                   type="A"))
        creative, _ = VideoCreative.objects.get_or_create(id=1)
        city, _ = GeoTarget.objects.get_or_create(id=1, defaults=dict(
            name="bobruisk"))
        ad = Ad.objects.create(id=1, ad_group=ad_group1)
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

    def test_success_for_chf_dashboard(self):
        user = self.create_test_user()
        user.add_custom_user_permission("view_dashboard")
        account = Account.objects.create(id=1, name="")
        self._create_stats(account)
        user_settings = {
            UserSettingsKey.VISIBLE_ACCOUNTS: [1],
        }
        with patch("aw_reporting.charts.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher), \
             self.patch_user_settings(**user_settings):
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

        self.assertAlmostEqual(sheet[SUMMARY_ROW_NUMBER][PerformanceReportColumn.COST].value, client_cost)
        self.assertAlmostEqual(sheet[SUMMARY_ROW_NUMBER + 1][PerformanceReportColumn.COST].value, client_cost)
        self.assertAlmostEqual(sheet[SUMMARY_ROW_NUMBER][PerformanceReportColumn.AVERAGE_CPM].value, average_cpm)
        self.assertAlmostEqual(sheet[SUMMARY_ROW_NUMBER][PerformanceReportColumn.AVERAGE_CPV].value, average_cpv)

    def test_hide_costs(self):
        user = self.create_test_user()
        any_date = date(2018, 1, 1)
        total_columns_count = len(PerformanceReport.columns)
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
        header_row_number = 1
        headers = tuple(cell.value for cell in sheet[header_row_number])
        expected_headers = (None, "Name", "Impressions", "Views", "Clicks", "Ctr(i)", "Ctr(v)", "View rate",
                            "25%", "50%", "75%", "100%")
        self.assertEqual(headers, expected_headers)
        row_lengths = [len(row) for row in sheet.rows]
        self.assertTrue(all([length == len(expected_headers) for length in row_lengths]))
        self.assertEqual(len(PerformanceReport.columns), total_columns_count)


def get_sheet_from_response(response):
    single_sheet_index = 0
    f = io.BytesIO(response.content)
    book = load_workbook(f)
    return book.worksheets[single_sheet_index]


SUMMARY_ROW_NUMBER = 2


def is_summary_empty(sheet):
    values_columns_indexes = range(2, 15)
    return all([sheet[SUMMARY_ROW_NUMBER][column].value is None for column in values_columns_indexes])


def is_empty_report(sheet):
    min_rows_count = 2
    return sheet.max_row <= min_rows_count and is_summary_empty(sheet)

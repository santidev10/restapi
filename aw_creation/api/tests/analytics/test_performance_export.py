import json
from datetime import date
from datetime import datetime
from datetime import timedelta
from itertools import chain
from itertools import product

from rest_framework.status import HTTP_200_OK

from aw_creation.api.urls.names import Name
from aw_creation.api.urls.namespace import Namespace
from aw_creation.models import AccountCreation
from aw_reporting.demo.data import DEMO_ACCOUNT_ID
from aw_reporting.excel_reports.analytics_performance_report import ALL_COLUMNS
from aw_reporting.excel_reports.analytics_performance_report import AnalyticsPerformanceReportColumn
from aw_reporting.models import AWAccountPermission
from aw_reporting.models import AWConnection
from aw_reporting.models import AWConnectionToUserRelation
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
from es_components.tests.utils import ESTestCase
from saas.urls.namespaces import Namespace as RootNamespace
from userprofile.constants import UserSettingsKey
from userprofile.constants import StaticPermissions
from utils.demo.recreate_test_demo_data import recreate_test_demo_data
from utils.unittests.int_iterator import int_iterator
from utils.unittests.reverse import reverse
from utils.unittests.test_case import ExtendedAPITestCase
from utils.unittests.xlsx import get_sheet_from_response


class AnalyticsPerformanceExportAPITestCase(ExtendedAPITestCase, ESTestCase):
    def setUp(self):
        self.user = self.create_test_user(perms={
            StaticPermissions.MANAGED_SERVICE__EXPORT: True,
            StaticPermissions.MANAGED_SERVICE: True,
        })

    def _get_url(self, account_creation_id):
        return reverse(Name.Analytics.PERFORMANCE_EXPORT, [RootNamespace.AW_CREATION, Namespace.ANALYTICS],
                       args=(account_creation_id,))

    def _request(self, account_creation_id, **kwargs):
        url = self._get_url(account_creation_id)
        return self.client.post(url, json.dumps(kwargs), content_type="application/json", )

    def _add_aw_connection(self, user):
        AWConnectionToUserRelation.objects.create(
            # user must have a connected account not to see demo data
            connection=AWConnection.objects.create(email="me@mail.kz",
                                                   refresh_token=""),
            user=user,
        )

    def _hide_demo_data_fallback(self, user):
        self._add_aw_connection(user)

    def _create_stats(self, account):
        campaign1 = Campaign.objects.create(id=1, name="#1", account=account)
        ad_group1 = AdGroup.objects.create(id=1, name="", campaign=campaign1)
        campaign2 = Campaign.objects.create(id=2, name="#2", account=account)
        ad_group2 = AdGroup.objects.create(id=2, name="", campaign=campaign2)
        action_date = datetime.now().date() - timedelta(days=1)
        base_stats = dict(date=action_date, impressions=100, video_views=10, cost=1)
        topic, _ = Topic.objects.get_or_create(id=1, defaults=dict(name="boo"))
        audience, _ = Audience.objects.get_or_create(id=1,
                                                     defaults=dict(name="boo",
                                                                   type="A"))
        creative, _ = VideoCreative.objects.get_or_create(id=1)
        city, _ = GeoTarget.objects.get_or_create(id=1, defaults=dict(
            name="Babruysk"))
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

    def assert_demo_data(self, response):
        self.assertEqual(response.status_code, HTTP_200_OK)
        sheet = get_sheet_from_response(response)

        self.assertFalse(is_empty_report(sheet))

        self.assertEqual(sheet[2][0].value, "Summary")
        self.assertIsNotNone(sheet[2][2].value)
        self.assertIsNotNone(sheet[3][0].value)

    def test_success(self):
        self._hide_demo_data_fallback(self.user)
        account = Account.objects.create(id=1, name="",
                                         skip_creating_account_creation=True)
        account_creation = AccountCreation.objects.create(name="", owner=self.user,
                                                          is_managed=False,
                                                          account=account,
                                                          is_approved=True)
        self._create_stats(account)

        today = datetime.now().date()
        filters = dict(
            start_date=str(today - timedelta(days=1)),
            end_date=str(today)
        )
        response = self._request(account_creation.id, **filters)
        sheet = get_sheet_from_response(response)
        self.assertFalse(is_empty_report(sheet))

    def test_success_demo(self):
        recreate_test_demo_data()

        today = datetime.now().date()
        filters = dict(
            start_date=str(today - timedelta(days=1)),
            end_date=str(today)
        )
        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
        }
        with self.patch_user_settings(**user_settings):
            response = self._request(DEMO_ACCOUNT_ID, **filters)
        self.assert_demo_data(response)

    def test_report_is_xlsx_formatted(self):
        self._hide_demo_data_fallback(self.user)
        account = Account.objects.create(id=1, name="",
                                         skip_creating_account_creation=True)
        account_creation = AccountCreation.objects.create(name="", owner=self.user,
                                                          is_managed=False,
                                                          account=account,
                                                          is_approved=True)
        self._create_stats(account)

        response = self._request(account_creation.id)
        self.assertEqual(response.status_code, HTTP_200_OK)
        try:
            get_sheet_from_response(response)
        # pylint: disable=broad-except
        except Exception:
            # pylint: enable=broad-except
            self.fail("Report is not an xls")

    def test_report_percent_formatted(self):
        self._hide_demo_data_fallback(self.user)
        account = Account.objects.create(id=1, name="",
                                         skip_creating_account_creation=True)
        account_creation = AccountCreation.objects.create(name="", owner=self.user,
                                                          is_managed=False,
                                                          account=account,
                                                          is_approved=True)
        self._create_stats(account)

        response = self._request(account_creation.id)
        self.assertEqual(response.status_code, HTTP_200_OK)
        sheet = get_sheet_from_response(response)
        self.assertFalse(is_empty_report(sheet))
        rows = range(2, sheet.max_row + 1)
        ctr_range = (ALL_COLUMNS.index(AnalyticsPerformanceReportColumn.CTR_I),
                     ALL_COLUMNS.index(AnalyticsPerformanceReportColumn.CTR_V) + 1)
        view_rate_range = (ALL_COLUMNS.index(AnalyticsPerformanceReportColumn.VIEW_RATE),
                           ALL_COLUMNS.index(AnalyticsPerformanceReportColumn.VIEW_RATE) + 1)
        quartile_range = (ALL_COLUMNS.index(AnalyticsPerformanceReportColumn.VIDEO_QUARTILE_25),
                          ALL_COLUMNS.index(AnalyticsPerformanceReportColumn.VIDEO_QUARTILE_100) + 1)
        test_ranges = [range(start, end) for start, end
                       in [ctr_range, view_rate_range, quartile_range]]
        cols = chain(*test_ranges)
        test_indexes = product(rows, cols)
        for row, column in test_indexes:
            cell = sheet[row][column]
            self.assertEqual(cell.number_format, "0.00%",
                             "Cell[{}:{}]".format(row, column))

    def test_aw_data_in_summary_row(self):
        self._hide_demo_data_fallback(self.user)
        any_date = date(2018, 1, 1)
        account = Account.objects.create(id=next(int_iterator), name="",
                                         skip_creating_account_creation=True)
        account_creation = AccountCreation.objects.create(name="", owner=self.user,
                                                          is_managed=False,
                                                          account=account,
                                                          is_approved=True)
        campaign = Campaign.objects.create(name="", account=account)
        ad_group = AdGroup.objects.create(campaign=campaign)
        impressions, views, aw_cost = 1234, 234, 12
        AdGroupStatistic.objects.create(date=any_date, ad_group=ad_group, average_position=1,
                                        cost=aw_cost, impressions=impressions, video_views=views)
        average_cpm = aw_cost / impressions * 1000
        average_cpv = aw_cost / views
        user_settings = {
            UserSettingsKey.VISIBLE_ACCOUNTS: [account.id],
            UserSettingsKey.DASHBOARD_AD_WORDS_RATES: False
        }
        with self.patch_user_settings(**user_settings):
            response = self._request(account_creation.id)
        self.assertEqual(response.status_code, HTTP_200_OK)
        sheet = get_sheet_from_response(response)
        self.assertFalse(is_empty_report(sheet))

        self.assertAlmostEqual(
            sheet[SUMMARY_ROW_NUMBER][ALL_COLUMNS.index(AnalyticsPerformanceReportColumn.COST)].value, aw_cost)
        self.assertAlmostEqual(
            sheet[SUMMARY_ROW_NUMBER + 1][ALL_COLUMNS.index(AnalyticsPerformanceReportColumn.COST)].value, aw_cost)
        self.assertAlmostEqual(
            sheet[SUMMARY_ROW_NUMBER][ALL_COLUMNS.index(AnalyticsPerformanceReportColumn.AVERAGE_CPM)].value,
            average_cpm)
        self.assertAlmostEqual(
            sheet[SUMMARY_ROW_NUMBER][ALL_COLUMNS.index(AnalyticsPerformanceReportColumn.AVERAGE_CPV)].value,
            average_cpv)

    def test_ignores_hide_costs(self):
        any_date = date(2018, 1, 1)
        opportunity = Opportunity.objects.create()
        placement = OpPlacement.objects.create(opportunity=opportunity, ordered_rate=.2, total_cost=23,
                                               goal_type_id=SalesForceGoalType.CPM)

        account = Account.objects.create(id=next(int_iterator), name="",
                                         skip_creating_account_creation=True)
        account_creation = AccountCreation.objects.create(name="", owner=self.user,
                                                          is_managed=False,
                                                          account=account,
                                                          is_approved=True)
        campaign = Campaign.objects.create(name="", account=account, salesforce_placement=placement)
        ad_group = AdGroup.objects.create(campaign=campaign)
        AdGroupStatistic.objects.create(date=any_date, ad_group=ad_group, average_position=1,
                                        cost=1, impressions=1, video_views=1)
        user_settings = {
            UserSettingsKey.VISIBLE_ACCOUNTS: [account.id],
            UserSettingsKey.DASHBOARD_COSTS_ARE_HIDDEN: True
        }
        with self.patch_user_settings(**user_settings):
            response = self._request(account_creation.id)
        self.assertEqual(response.status_code, HTTP_200_OK)
        sheet = get_sheet_from_response(response)
        self.assertFalse(is_empty_report(sheet))
        header_row_number = 1
        headers = tuple(cell.value for cell in sheet[header_row_number])
        expected_headers = (None, "Name", "Impressions", "Views", "Cost", "Average cpm", "Average cpv", "Clicks",
                            "Call-to-Action overlay", "Website", "App Store", "Cards", "End cap",
                            "Ctr(i)", "Ctr(v)", "View rate", "25%", "50%", "75%", "100%")
        self.assertEqual(headers, expected_headers)
        row_lengths = [len(row) for row in sheet.rows]
        self.assertTrue(all([length == len(expected_headers) for length in row_lengths]))

    def test_success_for_linked_account(self):
        self._add_aw_connection(self.user)
        manager = Account.objects.create(id=next(int_iterator))
        AWAccountPermission.objects.create(aw_connection=self.user.aw_connections.first().connection,
                                           account=manager)
        account = Account.objects.create(id=next(int_iterator))
        account.managers.add(manager)
        account.save()

        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True,
        }
        with self.patch_user_settings(**user_settings):
            response = self._request(account.account_creation.id)
        self.assertEqual(response.status_code, HTTP_200_OK)


SUMMARY_ROW_NUMBER = 2


def is_summary_empty(sheet):
    values_columns_indexes = range(2, 15)
    return all([sheet[SUMMARY_ROW_NUMBER][column].value is None for column in values_columns_indexes])


def is_empty_report(sheet):
    min_rows_count = 2
    return sheet.max_row <= min_rows_count and is_summary_empty(sheet)

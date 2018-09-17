import io
import re
from datetime import date
from datetime import timedelta
from unittest.mock import patch

from openpyxl import load_workbook
from rest_framework.status import HTTP_200_OK

from aw_creation.api.urls.names import Name
from aw_creation.api.urls.namespace import Namespace
from aw_reporting.excel_reports_dashboard import FOOTER_ANNOTATION
from aw_reporting.models import Account
from aw_reporting.models import AdGroup
from aw_reporting.models import Campaign
from aw_reporting.models import OpPlacement
from aw_reporting.models import Opportunity
from aw_reporting.models import VideoCreative
from aw_reporting.models import VideoCreativeStatistic
from aw_reporting.models import YTVideoStatistic
from saas.urls.namespaces import Namespace as RootNamespace
from userprofile.models import UserSettingsKey
from utils.utils_tests import ExtendedAPITestCase
from utils.utils_tests import SingleDatabaseApiConnectorPatcher
from utils.utils_tests import generic_test
from utils.utils_tests import int_iterator
from utils.utils_tests import patch_now
from utils.utils_tests import reverse


class SectionName:
    PLACEMENT = "Placement"
    VIDEOS = "Video"
    AD_GROUPS = "Ad Groups"
    INTERESTS = "Interests"
    TOPICS = "Topics"
    KEYWORDS = "Keywords"
    DEVICES = "Device"
    CREATIVES = "Creatives"


SECTIONS_WITH_CTA = (
    SectionName.PLACEMENT,
    SectionName.AD_GROUPS,
    SectionName.INTERESTS,
    SectionName.TOPICS,
    SectionName.KEYWORDS,
    SectionName.DEVICES,
)

REGULAR_STATISTIC_SECTIONS = (
    SectionName.VIDEOS,
    SectionName.CREATIVES,
)

COLUMN_SET_REGULAR = (
    "Impressions",
    "Views",
    "View Rate",
    "Clicks",
    "CTR",
    "Video played to: 25%",
    "Video played to: 50%",
    "Video played to: 75%",
    "Video played to: 100%",
    "Viewable Impressions",
    "Viewability",
)

COLUMN_SET_WITH_CTA = (
    "Impressions",
    "Views",
    "View Rate",
    "Clicks",
    "Call-to-Action overlay",
    "Website",
    "App Store",
    "Cards",
    "End cap",
    "CTR",
    "Video played to: 25%",
    "Video played to: 50%",
    "Video played to: 75%",
    "Video played to: 100%",
    "Viewable Impressions",
    "Viewability",
)

COLUMN_SET_BY_SECTION_NAME = {
    **{section_name: COLUMN_SET_REGULAR for section_name in REGULAR_STATISTIC_SECTIONS},
    **{section_name: COLUMN_SET_WITH_CTA for section_name in SECTIONS_WITH_CTA},
}


class DashboardWeeklyReportAPITestCase(ExtendedAPITestCase):
    def _get_url(self, account_creation_id):
        return reverse(
            Name.Dashboard.PERFORMANCE_EXPORT_WEEKLY_REPORT,
            [RootNamespace.AW_CREATION, Namespace.DASHBOARD],
            args=(account_creation_id,)
        )

    def _request(self, account_creation_id):
        url = self._get_url(account_creation_id)
        return self.client.post(url, "{}", content_type="application/json")

    def test_no_demo_data(self):
        self.create_test_user()
        account = Account.objects.create(id=next(int_iterator))
        campaign_name = "Test campaign"
        Campaign.objects.create(name=campaign_name)

        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True
        }
        with self.patch_user_settings(**user_settings):
            response = self._request(account.account_creation.id)
        self.assertEqual(response.status_code, HTTP_200_OK)
        sheet = get_sheet_from_response(response)
        self.assertTrue(is_report_empty(sheet))

    @generic_test([
        (section, (section,), dict())
        for section in SECTIONS_WITH_CTA
    ])
    def test_column_set(self, section):
        shared_columns = COLUMN_SET_BY_SECTION_NAME.get(section)
        self.create_test_user()
        account = Account.objects.create(id=next(int_iterator))

        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True
        }
        with self.patch_user_settings(**user_settings):
            response = self._request(account.account_creation.id)
        self.assertEqual(response.status_code, HTTP_200_OK)
        sheet = get_sheet_from_response(response)
        row_index = get_section_start_row(sheet, section)
        title_values = tuple(cell.value for cell in sheet[row_index][1:])
        self.assertEqual(title_values, (section,) + shared_columns)

    def test_video_section(self):
        any_date_1 = date(2018, 1, 1)
        any_date_2 = any_date_1 + timedelta(days=1)
        today = max(any_date_1, any_date_2)
        self.create_test_user()
        any_video = SingleDatabaseApiConnectorPatcher.get_video_list()["items"][0]
        video_title = any_video["title"]
        video_id = any_video["id"]
        account = Account.objects.create(id=next(int_iterator))
        campaign = Campaign.objects.create(account=account)
        ad_group = AdGroup.objects.create(campaign=campaign)
        impressions = (2, 3)
        YTVideoStatistic.objects.create(date=any_date_1, ad_group=ad_group, yt_id=video_id, impressions=impressions[0])
        YTVideoStatistic.objects.create(date=any_date_2, ad_group=ad_group, yt_id=video_id, impressions=impressions[1])

        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True
        }
        with patch_now(today), \
             self.patch_user_settings(**user_settings), \
             patch("aw_reporting.excel_reports_dashboard.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher):
            response = self._request(account.account_creation.id)
        self.assertEqual(response.status_code, HTTP_200_OK)
        sheet = get_sheet_from_response(response)
        row_index = get_section_start_row(sheet, SectionName.VIDEOS)
        data_row = sheet[row_index + 1]
        self.assertEqual(data_row[1].value, video_title)
        self.assertEqual(data_row[2].value, sum(impressions))

    def test_creatives_section(self):
        any_date_1 = date(2018, 1, 1)
        any_date_2 = any_date_1 + timedelta(days=1)
        today = max(any_date_1, any_date_2)
        self.create_test_user()
        any_video = SingleDatabaseApiConnectorPatcher.get_video_list()["items"][0]
        video_title = any_video["title"]
        video_id = any_video["id"]
        account = Account.objects.create(id=next(int_iterator))
        campaign = Campaign.objects.create(account=account)
        ad_group = AdGroup.objects.create(campaign=campaign)
        impressions = (2, 3)
        creative = VideoCreative.objects.create(id=video_id)
        common = dict(
            ad_group=ad_group,
            creative=creative,
        )
        VideoCreativeStatistic.objects.create(date=any_date_1, impressions=impressions[0], **common)
        VideoCreativeStatistic.objects.create(date=any_date_2, impressions=impressions[1], **common)

        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True
        }
        with patch_now(today), \
             self.patch_user_settings(**user_settings), \
             patch("aw_reporting.excel_reports_dashboard.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher):
            response = self._request(account.account_creation.id)
        self.assertEqual(response.status_code, HTTP_200_OK)
        sheet = get_sheet_from_response(response)
        row_index = get_section_start_row(sheet, SectionName.CREATIVES)
        data_row = sheet[row_index + 1]
        self.assertEqual(data_row[1].value, video_title)
        self.assertEqual(data_row[2].value, sum(impressions))

    def test_budget(self):
        self.create_test_user()
        opportunity = Opportunity.objects.create(budget=123.45678)
        placement = OpPlacement.objects.create(opportunity=opportunity)
        account = Account.objects.create(id=next(int_iterator))
        Campaign.objects.create(account=account, salesforce_placement=placement)
        user_settings = {
            UserSettingsKey.VISIBLE_ALL_ACCOUNTS: True
        }

        with self.patch_user_settings(**user_settings):
            response = self._request(account.account_creation.id)
        self.assertEqual(response.status_code, HTTP_200_OK)
        sheet = get_sheet_from_response(response)
        header_value = get_title_cell(sheet).value
        self.assertIn("Client Budget: ${}".format(opportunity.budget), header_value)


def get_sheet_from_response(response):
    single_sheet_index = 0
    f = io.BytesIO(response.content)
    book = load_workbook(f)
    return book.worksheets[single_sheet_index]


TITLE_COLUMN = 1
TOTAL_NAME = "Total"
FIRST_SECTION_ROW_NUMBER = 13


def get_title_cell(sheet):
    return sheet[5][1]


def is_title_empty(sheet):
    title_cell = get_title_cell(sheet)
    return re.match(r"^Campaign: (None)?\n.*", title_cell.value) is not None


def is_demo_report(sheet):
    title_cell = get_title_cell(sheet)
    return re.match(r"^Campaign: Demo\n.*", title_cell.value) is not None and not is_title_empty(sheet)


def is_report_empty(sheet):
    return is_title_empty(sheet) and are_all_sections_empty(sheet)


def are_all_sections_empty(sheet):
    section_names = (
        (SectionName.PLACEMENT, "Total"),
        (SectionName.VIDEOS, None),
        (SectionName.CREATIVES, None),
        (SectionName.AD_GROUPS, None),
        (SectionName.INTERESTS, None),
        (SectionName.TOPICS, None),

        (SectionName.KEYWORDS, None),
        (SectionName.DEVICES, FOOTER_ANNOTATION),
    )
    return all([
        is_section_empty(sheet, section_name, next_value)
        for section_name, next_value in section_names
    ])


def is_section_empty(sheet, section_name, next_value):
    section_row_number = get_section_start_row(sheet, section_name)
    next_row_number = section_row_number + 1
    return sheet[next_row_number][TITLE_COLUMN].value == next_value


def get_section_start_row(sheet, section_name):
    data_rows_numbers = range(FIRST_SECTION_ROW_NUMBER, sheet.max_row)
    for row_number in data_rows_numbers:
        if sheet[row_number][TITLE_COLUMN].value == section_name:
            return row_number
    raise ValueError

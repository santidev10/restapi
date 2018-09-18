import io
import re

from openpyxl import load_workbook
from rest_framework.status import HTTP_200_OK

from aw_creation.api.urls.names import Name
from aw_creation.api.urls.namespace import Namespace
from aw_reporting.excel_reports_dashboard import FOOTER_ANNOTATION
from aw_reporting.models import Account
from aw_reporting.models import Campaign
from saas.urls.namespaces import Namespace as RootNamespace
from userprofile.models import UserSettingsKey
from utils.utils import get_all_class_constants
from utils.utils_tests import ExtendedAPITestCase
from utils.utils_tests import generic_test
from utils.utils_tests import int_iterator
from utils.utils_tests import reverse


class SectionName:
    PLACEMENT = "Placement"
    AD_GROUPS = "Ad Groups"
    INTERESTS = "Interests"
    TOPICS = "Topics"
    KEYWORDS = "Keywords"
    DEVICES = "Device"


ALL_SECTIONS = get_all_class_constants(SectionName)


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
        for section in ALL_SECTIONS
    ])
    def test_column_set(self, section):
        shared_columns = (
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

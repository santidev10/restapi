import io
import re
from itertools import product

from openpyxl import load_workbook
from rest_framework.status import HTTP_200_OK

from aw_creation.api.urls.names import Name
from aw_creation.api.urls.namespace import Namespace
from aw_creation.models import AccountCreation
from aw_reporting.demo.models import DEMO_ACCOUNT_ID
from aw_reporting.models import Account
from aw_reporting.models import Campaign
from saas.urls.namespaces import Namespace as RootNamespace
from utils.utittests.generic_test import generic_test
from utils.utittests.int_iterator import int_iterator
from utils.utittests.reverse import reverse
from utils.utittests.test_case import ExtendedAPITestCase


class SectionName:
    AD_GROUPS = "Ad Groups"
    PLACEMENT = "Placement"
    INTERESTS = "Interests"
    TOPICS = "Topics"
    DEVICES = "Device"
    KEYWORDS = "Keywords"


STATISTIC_SECTIONS = (
    SectionName.AD_GROUPS,
    SectionName.DEVICES,
    SectionName.INTERESTS,
    SectionName.KEYWORDS,
    SectionName.PLACEMENT,
    SectionName.TOPICS,
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
)

COLUMN_SET_BY_SECTION_NAME = {
    **{section_name: COLUMN_SET_WITH_CTA for section_name in STATISTIC_SECTIONS},
}


class AnalyticsWeeklyReportAPITestCase(ExtendedAPITestCase):
    def _get_url(self, account_creation_id):
        return reverse(
            Name.Analytics.PERFORMANCE_EXPORT_WEEKLY_REPORT,
            [RootNamespace.AW_CREATION, Namespace.ANALYTICS],
            args=(account_creation_id,)
        )

    def test_success(self):
        user = self.create_test_user()
        account = Account.objects.create(id=1, name="",
                                         skip_creating_account_creation=True)
        account_creation = AccountCreation.objects.create(name="", owner=user,
                                                          is_managed=False,
                                                          account=account)

        url = self._get_url(account_creation.id)
        response = self.client.post(url)
        self.assertEqual(response.status_code, HTTP_200_OK)

    def test_success_demo(self):
        self.create_test_user()
        url = self._get_url(DEMO_ACCOUNT_ID)
        response = self.client.post(url)
        self.assertEqual(response.status_code, HTTP_200_OK)

    def test_quartiles_are_percent_formatted(self):
        self.create_test_user()
        url = self._get_url(DEMO_ACCOUNT_ID)
        response = self.client.post(url)
        f = io.BytesIO(response.content)
        book = load_workbook(f)
        sheet = book.worksheets[0]

        row_indexes = range(14, 17)
        column_indexes = range(7, 11)
        cell_indexes = product(row_indexes, column_indexes)
        for row, column in cell_indexes:
            cell = sheet[row][column]
            self.assertEqual(cell.number_format, "0.00%",
                             "Cell[{}:{}]".format(row, column))

    def test_view_rate_are_percent_formatted(self):
        self.create_test_user()
        url = self._get_url(DEMO_ACCOUNT_ID)
        response = self.client.post(url)
        f = io.BytesIO(response.content)
        book = load_workbook(f)
        sheet = book.worksheets[0]

        row_indexes = range(14, 17)
        column_indexes = [4]
        cell_indexes = product(row_indexes, column_indexes)
        for row, column in cell_indexes:
            cell = sheet[row][column]
            self.assertEqual(cell.number_format, "0.00%",
                             "Cell[{}:{}]".format(row, column))

    def test_ctr_are_percent_formatted(self):
        self.create_test_user()
        url = self._get_url(DEMO_ACCOUNT_ID)
        response = self.client.post(url)
        f = io.BytesIO(response.content)
        book = load_workbook(f)
        sheet = book.worksheets[0]

        row_indexes = range(14, 17)
        column_indexes = [6]
        cell_indexes = product(row_indexes, column_indexes)
        for row, column in cell_indexes:
            cell = sheet[row][column]
            self.assertEqual(cell.number_format, "0.00%",
                             "Cell[{}:{}]".format(row, column))

    def test_demo_data(self):
        user = self.create_test_user()
        account = Account.objects.create(id=next(int_iterator),
                                         skip_creating_account_creation=True)
        account_creation = AccountCreation.objects.create(owner=user,
                                                          is_managed=False,
                                                          account=account)
        campaign_name = "Test campaign"
        Campaign.objects.create(name=campaign_name)

        url = self._get_url(account_creation.id)
        response = self.client.post(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        sheet = get_sheet_from_response(response)
        is_demo_report(sheet)

    @generic_test([
        (section, (section,), dict())
        for section in STATISTIC_SECTIONS
    ])
    def test_column_set(self, section):
        shared_columns = COLUMN_SET_BY_SECTION_NAME.get(section)
        user = self.create_test_user()
        account = Account.objects.create(id=next(int_iterator),
                                         skip_creating_account_creation=True)
        account_creation = AccountCreation.objects.create(owner=user,
                                                          is_managed=False,
                                                          account=account)

        url = self._get_url(account_creation.id)
        response = self.client.post(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        sheet = get_sheet_from_response(response)
        row_index = get_section_start_row(sheet, section)
        title_values = tuple(cell.value for cell in sheet[row_index][1:])
        self.assertEqual(title_values, (section,) + shared_columns)

    @generic_test([
        (section, (section,), dict())
        for section in STATISTIC_SECTIONS
    ])
    def test_demo_account_cta(self, section):
        shared_columns = COLUMN_SET_BY_SECTION_NAME.get(section)
        self.create_test_user()

        url = self._get_url(DEMO_ACCOUNT_ID)
        response = self.client.post(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        sheet = get_sheet_from_response(response)
        row_index = get_section_start_row(sheet, section)
        title_values = tuple(cell.value for cell in sheet[row_index][1:])
        self.assertEqual(title_values, (section,) + shared_columns)


TITLE_COLUMN = 1
FIRST_SECTION_ROW_NUMBER = 13


def get_sheet_from_response(response):
    single_sheet_index = 0
    f = io.BytesIO(response.content)
    book = load_workbook(f)
    return book.worksheets[single_sheet_index]


def get_title_cell(sheet):
    return sheet[5][1]


def is_title_empty(sheet):
    title_cell = get_title_cell(sheet)
    return re.match(r"^Campaign: (None)?\n.*", title_cell.value) is not None


def is_demo_report(sheet):
    title_cell = get_title_cell(sheet)
    return re.match(r"^Campaign: Demo\n.*", title_cell.value) is not None and not is_title_empty(sheet)


def is_section_header(cell):
    return cell.alignment.horizontal == "center"


def get_section_start_row(sheet, section_name):
    data_rows_numbers = range(FIRST_SECTION_ROW_NUMBER, sheet.max_row)
    for row_number in data_rows_numbers:
        cell = sheet[row_number][TITLE_COLUMN]
        if is_section_header(cell) and cell.value == section_name:
            return row_number
    raise ValueError

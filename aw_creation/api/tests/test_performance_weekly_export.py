import io
import json
from itertools import product

from django.core.urlresolvers import reverse
from django.http import HttpResponse
from openpyxl import load_workbook
from rest_framework.status import HTTP_200_OK

from aw_creation.api.urls.names import Name
from aw_creation.models import AccountCreation
from aw_reporting.demo.models import DEMO_ACCOUNT_ID
from aw_reporting.models import Account, Campaign
from saas.urls.namespaces import Namespace
from utils.utils_tests import ExtendedAPITestCase


class AnalyzeExportAPITestCase(ExtendedAPITestCase):
    def _get_url(self, account_creation_id):
        return reverse(
            Namespace.AW_CREATION + ":" + Name.Dashboard.PERFORMANCE_EXPORT_WEEKLY_REPORT,
            args=(account_creation_id,))


class AnalyzeExportAnalyticsAPITestCase(AnalyzeExportAPITestCase):
    def test_success(self):
        user = self.create_test_user()
        account = Account.objects.create(id=1, name="")
        account_creation = AccountCreation.objects.create(name="", owner=user,
                                                          is_managed=False,
                                                          account=account)

        url = self._get_url(account_creation.id)
        response = self.client.post(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(type(response), HttpResponse)

    def test_success_demo(self):
        self.create_test_user()
        url = self._get_url(DEMO_ACCOUNT_ID)
        response = self.client.post(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(type(response), HttpResponse)

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
        account = Account.objects.create(id=1, name="")
        account_creation = AccountCreation.objects.create(name="", owner=user,
                                                          is_managed=False,
                                                          account=account)
        campaign_name = "Test campaign"
        Campaign.objects.create(name=campaign_name)

        url = self._get_url(account_creation.id)
        response = self.client.post(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        f = io.BytesIO(response.content)
        book = load_workbook(f)
        sheet = book.worksheets[0]

        title_cell = sheet[5][1].value
        campaign_name_value = title_cell.split("\n")[0]
        self.assertEqual(campaign_name_value, "Campaign: Demo")


class AnalyzeExportDashboardAPITestCase(AnalyzeExportAPITestCase):
    def _request(self, account_creation_id):
        url = self._get_url(account_creation_id)
        return self.client.post(url, json.dumps(dict(is_chf=1)), content_type="application/json")

    def test_no_demo_data(self):
        user = self.create_test_user()
        account = Account.objects.create(id=1, name="")
        account_creation = AccountCreation.objects.create(name="", owner=user,
                                                          is_managed=False,
                                                          account=account)
        campaign_name = "Test campaign"
        Campaign.objects.create(name=campaign_name)

        response = self._request(account_creation.id)
        self.assertEqual(response.status_code, HTTP_200_OK)
        f = io.BytesIO(response.content)
        book = load_workbook(f)
        sheet = book.worksheets[0]

        title_cell = sheet[5][1].value
        campaign_name_value = title_cell.split("\n")[0]
        self.assertEqual(campaign_name_value, "Campaign: ")
        self.assertEqual(sheet[13][1].value, "Placement")
        self.assertEqual(sheet[14][1].value, "Total")

        sections = [
            "Ad Groups",
            "Interests",
            "Topics",
            "Keywords",
            "Device"
        ]
        section_starts = zip(sections, range(16, 50, 2))
        for section, row_index in section_starts:
            self.assertEqual(sheet[row_index][1].value, section)

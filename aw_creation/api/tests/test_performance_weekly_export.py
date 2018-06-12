import io
from itertools import product

from django.core.urlresolvers import reverse
from django.http import HttpResponse
from openpyxl import load_workbook
from rest_framework.status import HTTP_200_OK

from aw_creation.api.urls.names import Name
from aw_creation.models import AccountCreation
from aw_reporting.demo.models import DEMO_ACCOUNT_ID
from aw_reporting.models import Account
from saas.urls.namespaces import Namespace
from utils.utils_tests import ExtendedAPITestCase


class AnalyzeExportAPITestCase(ExtendedAPITestCase):
    def _get_url(self, account_creation_id):
        return reverse(
            Namespace.AW_CREATION + ":" + Name.Dashboard.PERFORMANCE_EXPORT_WEEKLY_REPORT,
            args=(account_creation_id,))

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
        url = reverse("aw_creation_urls:performance_export_weekly_report",
                      args=(DEMO_ACCOUNT_ID,))
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
        url = reverse("aw_creation_urls:performance_export_weekly_report",
                      args=(DEMO_ACCOUNT_ID,))
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

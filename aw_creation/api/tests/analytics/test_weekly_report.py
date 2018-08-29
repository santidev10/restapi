import io
import re
from itertools import product

from django.http import HttpResponse
from openpyxl import load_workbook
from rest_framework.status import HTTP_200_OK

from aw_creation.api.urls.names import Name
from aw_creation.api.urls.namespace import Namespace
from aw_creation.models import AccountCreation
from aw_reporting.demo.models import DEMO_ACCOUNT_ID
from aw_reporting.models.ad_words import Account
from aw_reporting.models.ad_words import Campaign
from aw_reporting.models.salesforce import Flight
from aw_reporting.models.salesforce import OpPlacement
from aw_reporting.models.salesforce import Opportunity
from aw_reporting.models.salesforce_constants import SalesForceGoalType
from saas.urls.namespaces import Namespace as RootNamespace
from utils.utils_tests import ExtendedAPITestCase
from utils.utils_tests import int_iterator
from utils.utils_tests import reverse


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
        self.create_test_user()

        url = self._get_url(DEMO_ACCOUNT_ID)
        response = self.client.post(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        sheet = get_sheet_from_response(response)
        self.assertTrue(is_demo_report(sheet))

    def test_header_budget(self):
        user = self.create_test_user()
        account = Account.objects.create(id=next(int_iterator),
                                         skip_creating_account_creation=True)
        account_creation = AccountCreation.objects.create(owner=user,
                                                          is_managed=False,
                                                          account=account)
        opportunity = Opportunity.objects.create(id=next(int_iterator))
        placement_cpv = OpPlacement.objects.create(id=next(int_iterator), opportunity=opportunity,
                                                   goal_type_id=SalesForceGoalType.CPV)
        placement_cpm = OpPlacement.objects.create(id=next(int_iterator), opportunity=opportunity,
                                                   goal_type_id=SalesForceGoalType.CPM)
        Flight.objects.create(id=next(int_iterator), placement=placement_cpv, total_cost=1)
        Flight.objects.create(id=next(int_iterator), placement=placement_cpv, total_cost=2)
        Flight.objects.create(id=next(int_iterator), placement=placement_cpm, total_cost=3)
        Flight.objects.create(id=next(int_iterator), placement=placement_cpm, total_cost=4)

        Campaign.objects.create(id=next(int_iterator), name="", account=account, salesforce_placement=placement_cpv)
        Campaign.objects.create(id=next(int_iterator), name="", account=account, salesforce_placement=placement_cpm)

        expected_budget = sum([
            f.total_cost
            for f in Flight.objects.filter(placement__opportunity=opportunity)
        ])
        self.assertGreater(expected_budget, 0)

        url = self._get_url(account_creation.id)
        response = self.client.post(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        sheet = get_sheet_from_response(response)
        budget = get_title_value(sheet, "Budget", float)
        self.assertEqual(budget, expected_budget)

    def test_header_cpv(self):
        user = self.create_test_user()
        account = Account.objects.create(id=next(int_iterator),
                                         skip_creating_account_creation=True)
        account_creation = AccountCreation.objects.create(owner=user,
                                                          is_managed=False,
                                                          account=account)
        opportunity = Opportunity.objects.create(id=next(int_iterator), cpv_cost=3, video_views=4)
        placement = OpPlacement.objects.create(id=next(int_iterator), opportunity=opportunity)
        Campaign.objects.create(id=next(int_iterator), account=account, name="", salesforce_placement=placement)

        expected_cpv = opportunity.cpv
        self.assertGreater(expected_cpv, 0)

        url = self._get_url(account_creation.id)
        response = self.client.post(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        sheet = get_sheet_from_response(response)
        cpv = get_title_value(sheet, "CPV", float)
        self.assertEqual(cpv, expected_cpv)

    def test_header_contracted_views(self):
        user = self.create_test_user()
        account = Account.objects.create(id=next(int_iterator),
                                         skip_creating_account_creation=True)
        account_creation = AccountCreation.objects.create(owner=user,
                                                          is_managed=False,
                                                          account=account)
        opportunity = Opportunity.objects.create(id=next(int_iterator))
        placement_cpv = OpPlacement.objects.create(id=next(int_iterator), opportunity=opportunity,
                                                   goal_type_id=SalesForceGoalType.CPV)
        placement_cpm = OpPlacement.objects.create(id=next(int_iterator), opportunity=opportunity,
                                                   goal_type_id=SalesForceGoalType.CPM)
        Flight.objects.create(id=next(int_iterator), placement=placement_cpv, ordered_units=1)
        Flight.objects.create(id=next(int_iterator), placement=placement_cpv, ordered_units=2)
        Flight.objects.create(id=next(int_iterator), placement=placement_cpm, ordered_units=3)
        Flight.objects.create(id=next(int_iterator), placement=placement_cpm, ordered_units=4)

        Campaign.objects.create(id=next(int_iterator), account=account, name="", salesforce_placement=placement_cpv)
        Campaign.objects.create(id=next(int_iterator), account=account, name="", salesforce_placement=placement_cpm)

        expected_contracted_views = sum([
            f.ordered_units
            for f in Flight.objects.filter(placement__opportunity=opportunity,
                                           placement__goal_type_id=SalesForceGoalType.CPV)
        ])

        url = self._get_url(account_creation.id)
        response = self.client.post(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        sheet = get_sheet_from_response(response)
        contracted_views = get_title_value(sheet, "Contracted Views", int)
        self.assertEqual(contracted_views, expected_contracted_views)

    def test_header_na_if_no_data(self):
        user = self.create_test_user()
        account = Account.objects.create(id=next(int_iterator),
                                         skip_creating_account_creation=True)
        account_creation = AccountCreation.objects.create(owner=user,
                                                          is_managed=False,
                                                          account=account)
        opportunity = Opportunity.objects.create(id=next(int_iterator))
        placement = OpPlacement.objects.create(id=next(int_iterator), opportunity=opportunity)
        Campaign.objects.create(id=next(int_iterator), account=account, name="", salesforce_placement=placement)

        url = self._get_url(account_creation.id)
        response = self.client.post(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        sheet = get_sheet_from_response(response)
        self.assertEqual(get_title_value(sheet, "Budget", str), "N/A")
        self.assertEqual(get_title_value(sheet, "CPV", str), "N/A")
        self.assertEqual(get_title_value(sheet, "CPV", str), "N/A")


def get_sheet_from_response(response):
    single_sheet_index = 0
    f = io.BytesIO(response.content)
    book = load_workbook(f)
    return book.worksheets[single_sheet_index]


def get_title_cell(sheet):
    return sheet[5][1]


def get_title_value(sheet, label, tp):
    header = get_title_cell(sheet).value
    value_search = re.search(re.compile("{}: (.+)\n".format(label)), header)
    if not value_search:
        return None
    try:
        return tp(value_search.group(1))
    except ValueError:
        return None


def is_title_empty(sheet):
    title_cell = get_title_cell(sheet)
    return re.match(r"^Campaign: (None)?\n.*", title_cell.value) is not None


def is_demo_report(sheet):
    title_cell = get_title_cell(sheet)
    return re.match(r"^Campaign: Demo\n.*", title_cell.value) is not None and not is_title_empty(sheet)

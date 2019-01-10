from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK

from aw_reporting.api.urls.names import Name
from saas.urls.namespaces import Namespace
from utils.utittests.test_case import ExtendedAPITestCase
from utils.utittests.xlsx import get_sheet_from_response


class PacingReportExportTestCase(ExtendedAPITestCase):
    url = reverse(Namespace.AW_REPORTING + ":" + Name.PacingReport.EXPORT)

    def test_success(self):
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)

    def test_headers(self):
        response = self.client.get(self.url)

        sheet = get_sheet_from_response(response)

        headers = tuple([cell.value for cell in list(sheet.rows)[1]])
        self.assertEqual(headers, COLUMNS)


COLUMNS = (
    # Name
    "Opportunity",
    "Placement",
    "Flight",
    "Campaign",

    # KPIs
    "Pacing",
    "Margin",

    # Dates
    "IO",
    "Start",
    "End",

    # Goals
    "Budget",
    "Views",
    "CPV",
    "Impressions",
    "CPM",

    # Delivered
    "Cost",
    "Views",
    "CPV",
    "Impressions",
    "CPM",

    "AdOps",
    "AM",
    "Sales",
    "Category",
    "Territory",
)

from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK

from aw_reporting.api.urls.names import Name
from saas.urls.namespaces import Namespace
from utils.utittests.test_case import ExtendedAPITestCase
from utils.utittests.csv import get_data_from_csv_response


class PacingReportExportTestCase(ExtendedAPITestCase):
    url = reverse(Namespace.AW_REPORTING + ":" + Name.PacingReport.EXPORT)

    def test_success(self):
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response["Content-Type"], "text/csv")

    def test_headers(self):
        response = self.client.get(self.url)

        csv_data = get_data_from_csv_response(response)

        headers = next(csv_data)
        self.assertEqual(headers, COLUMNS)


COLUMNS = [
    # Name
    "Name.Opportunity",
    "Name.Placement",
    "Name.Flight",
    "Name.Campaign",

    # KPIs
    "KPIs.Pacing",
    "KPIs.Margin",

    # Dates
    "Dates.IO",
    "Dates.Start",
    "Dates.End",

    # Goals
    "Goals.Budget",
    "Goals.Views",
    "Goals.CPV",
    "Goals.Impressions",
    "Goals.CPM",

    # Delivered
    "Delivered.Cost",
    "Delivered.Views",
    "Delivered.CPV",
    "Delivered.Impressions",
    "Delivered.CPM",

    "AdOps",
    "AM",
    "Sales",
    "Category",
    "Territory",
]

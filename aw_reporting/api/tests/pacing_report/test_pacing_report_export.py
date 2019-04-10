from unittest import mock

from rest_framework.status import HTTP_200_OK

from aw_reporting.api.urls.names import Name
from aw_reporting.csv_reports import PacingReportCSVExport
from aw_reporting.reports.pacing_report import PacingReport
from saas.urls.namespaces import Namespace
from utils.utittests.csv import get_data_from_csv_response
from utils.utittests.s3_mock import mock_s3
from utils.utittests.test_case import ExtendedAPITestCase
from utils.utittests.reverse import reverse


class PacingReportExportTestCase(ExtendedAPITestCase):

    @mock_s3
    @mock.patch("aw_reporting.reports.pacing_report.PacingReport.get_opportunities", return_value=[])
    def test_success(self, *args, **kwargs):
        report_name = "PacingReport-test"
        pacing_report = PacingReport()
        opportunities = pacing_report.get_opportunities()

        csv_generator = PacingReportCSVExport(pacing_report, opportunities, report_name)
        csv_generator.export_to_s3()

        url = reverse(Name.PacingReport.EXPORT, [Namespace.AW_REPORTING], args=(report_name,))

        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response["Content-Type"], "application/CSV")

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

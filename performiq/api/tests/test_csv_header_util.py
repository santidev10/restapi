from django.test import testcases
from performiq.utils.map_csv_fields import CSVHeaderUtil
from performiq.utils.map_csv_fields import CSVWithHeader
from performiq.utils.map_csv_fields import CSVWithOnlyData
from performiq.utils.map_csv_fields import ManagedPlacementsReport


class MapCSVFieldsValidatorTestCase(testcases.TestCase):

    def test_managed_placements_report_failure(self):
        reports = {
            "Placements reports must have at least 4 rows": [
                ["Managed placements report"],
                ["All time"],
                ["Placement Status", "Placement", "Placement URL", "asdf"],
            ],
            "CSV must have at least one column": [
                [],
                [],
                [],
                [],
            ],
            "First row must be 'Managed placements report'": [
                ["Managed asdf report"],
                ["All time"],
                ["Placement Status", "Placement", "Placement URL", "asdf"],
                [123, 432, 0.34, "y", "n"],
            ],
            "Third row must be a header row": [
                ["Managed placements report"],
                ["All time"],
                [123, 432, 0.34, "y", "n"],
                [123, 432, 0.34, "y", "n"],
            ],
        }
        validation_error_key = ManagedPlacementsReport.get_type_string()
        for expected_validation_message, rows in reports.items():
            with self.subTest(rows):
                util = CSVHeaderUtil(rows=rows)
                self.assertIn(expected_validation_message, util.validation_errors[validation_error_key])

    def test_managed_placements_report_success(self):
        reports = {
            "Second can be 'All time'": [
                ["Managed placements report"],
                ["All time"],
                ["Placement Status", "Placement", "Placement URL", "asdf"],
                [123, 432, 0.34, "y", "n"],
            ],
            "Second can January Date Range": [
                ["Managed placements report"],
                ["January 7, 2020 - January 10, 2020"],
                ["Placement Status", "Placement", "Placement URL", "asdf"],
                [123, 432, 0.34, "y", "n"],
            ],
            "Second can February Date Range": [
                ["Managed placements report"],
                ["February 15, 2020 - February 16, 2020"],
                ["Placement Status", "Placement", "Placement URL", "asdf"],
                [123, 432, 0.34, "y", "n"],
            ],
            "Second can March Date Range": [
                ["Managed placements report"],
                ["March 1, 2020 - March 29, 2020"],
                ["Placement Status", "Placement", "Placement URL", "asdf"],
                [123, 432, 0.34, "y", "n"],
            ],
            "Second can April Date Range": [
                ["Managed placements report"],
                ["April 18, 2020 - April 27, 2020"],
                ["Placement Status", "Placement", "Placement URL", "asdf"],
                [123, 432, 0.34, "y", "n"],
            ],
            "Second can May Date Range": [
                ["Managed placements report"],
                ["May 2, 2020 - May 5, 2020"],
                ["Placement Status", "Placement", "Placement URL", "asdf"],
                [123, 432, 0.34, "y", "n"],
            ],
            "Second can June Date Range": [
                ["Managed placements report"],
                ["June 14, 2020 - June 20, 2020"],
                ["Placement Status", "Placement", "Placement URL", "asdf"],
                [123, 432, 0.34, "y", "n"],
            ],
            "Second can July Date Range": [
                ["Managed placements report"],
                ["July 4, 2020 - July 4, 2020"],
                ["Placement Status", "Placement", "Placement URL", "asdf"],
                [123, 432, 0.34, "y", "n"],
            ],
            "Second can August Date Range": [
                ["Managed placements report"],
                ["August 1, 2020 - August 1, 2020"],
                ["Placement Status", "Placement", "Placement URL", "asdf"],
                [123, 432, 0.34, "y", "n"],
            ],
            "Second can September Date Range": [
                ["Managed placements report"],
                ["September 20, 2020 - September 20, 2020"],
                ["Placement Status", "Placement", "Placement URL", "asdf"],
                [123, 432, 0.34, "y", "n"],
            ],
            "Second can October Date Range": [
                ["Managed placements report"],
                ["October 30, 2020 - October 31, 2020"],
                ["Placement Status", "Placement", "Placement URL", "asdf"],
                [123, 432, 0.34, "y", "n"],
            ],
            "Second can November Date Range": [
                ["Managed placements report"],
                ["November 2, 2020 - November 22, 2020"],
                ["Placement Status", "Placement", "Placement URL", "asdf"],
                [123, 432, 0.34, "y", "n"],
            ],
            "Second can December Date Range": [
                ["Managed placements report"],
                ["December 11, 2020 - December 11, 2020"],
                ["Placement Status", "Placement", "Placement URL", "asdf"],
                [123, 432, 0.34, "y", "n"],
            ],
        }
        for expected_validation_message, rows in reports.items():
            with self.subTest(rows):
                util = CSVHeaderUtil(rows=rows)
                self.assertEqual(util.get_first_data_row_index(), 3)

    def test_csv_with_header_row_failure(self):
        reports = {
            "CSV must have at least two rows": [
                ["x", "y", "z"],
            ],
            "CSV must have at least one column": [
                [],
                [],
            ],
            "First row must be a header row": [
                [123, 432, 0.34, "y", "n"],
                [123, 432, 0.34, "y", "n"],
            ],
            "Second row must be a data row": [
                ["x", "y", "z"],
                ["x", "y", "z"],
            ],
        }
        validation_error_key = CSVWithHeader.get_type_string()
        for expected_validation_message, rows in reports.items():
            with self.subTest(rows):
                util = CSVHeaderUtil(rows=rows)
                self.assertIn(expected_validation_message, util.validation_errors[validation_error_key])

    def test_csv_with_header_row_success(self):
        rows = [
            ["Placement Status", "Placement", "Placement URL", "asdf"],
            [123, 432, 0.34, "y", "n"],
        ]
        util = CSVHeaderUtil(rows=rows)
        self.assertEqual(util.get_first_data_row_index(), 1)

    def test_csv_with_only_data_failure(self):
        reports = {
            "CSV must have at least one row": [],
            "CSV must have at least one column": [
                [],
            ],
            "CSV cannot have a header row": [
                ["Placement Status", "Placement", "Placement URL", "asdf"],
            ],
        }
        validation_error_key = CSVWithOnlyData.get_type_string()
        for expected_validation_message, rows in reports.items():
            with self.subTest(rows):
                util = CSVHeaderUtil(rows=rows)
                self.assertIn(expected_validation_message, util.validation_errors[validation_error_key])

    def test_csv_with_only_data_success(self):
        rows = [
            [123, 432, 0.34, "y", "n"],
        ]
        util = CSVHeaderUtil(rows=rows)
        self.assertEqual(util.get_first_data_row_index(), 0)

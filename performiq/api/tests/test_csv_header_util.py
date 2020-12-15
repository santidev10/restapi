from django.test import testcases
from performiq.utils.map_csv_fields import CSVHeaderUtil


class MapCSVFieldsValidatorTestCase(testcases.TestCase):

    def test_managed_placements_report_failure(self):
        reports = {
            "Managed placements reports must have at least 4 rows": [
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
            "First row is not 'Managed placements report'": [
                ["Managed asdf report"],
                ["All time"],
                ["Placement Status", "Placement", "Placement URL", "asdf"],
                [123, 432, 0.34, "y", "n"],
            ],
            "Second row is not 'All time'": [
                ["Managed placements report"],
                ["adsf"],
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
        for expected_validation_message, rows in reports.items():
            with self.subTest(rows):
                util = CSVHeaderUtil(rows=rows)
                validation_messages = [error.message for error in util.validation_errors]
                self.assertIn(expected_validation_message, validation_messages)

    def test_managed_placements_report_success(self):
        rows = [
            ["Managed placements report"],
            ["All time"],
            ["Placement Status", "Placement", "Placement URL", "asdf"],
            [123, 432, 0.34, "y", "n"],
        ]
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
            "First row is not a header row": [
                [123, 432, 0.34, "y", "n"],
                [123, 432, 0.34, "y", "n"],
            ],
            "Second row is a header row": [
                ["x", "y", "z"],
                ["x", "y", "z"],
            ],
        }
        for expected_validation_message, rows in reports.items():
            with self.subTest(rows):
                util = CSVHeaderUtil(rows=rows)
                validation_messages = [error.message for error in util.validation_errors]
                self.assertIn(expected_validation_message, validation_messages)

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
            "CSV has a header row": [
                ["Placement Status", "Placement", "Placement URL", "asdf"],
            ],
        }
        for expected_validation_message, rows in reports.items():
            with self.subTest(rows):
                util = CSVHeaderUtil(rows=rows)
                validation_messages = [error.message for error in util.validation_errors]
                self.assertIn(expected_validation_message, validation_messages)

    def test_csv_with_only_data_success(self):
        rows = [
            [123, 432, 0.34, "y", "n"],
        ]
        util = CSVHeaderUtil(rows=rows)
        self.assertEqual(util.get_first_data_row_index(), 0)

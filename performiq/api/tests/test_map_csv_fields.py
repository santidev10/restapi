import csv
import os
import string

import boto3
from django.urls import reverse
from django.conf import settings
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_400_BAD_REQUEST
from rest_framework.status import HTTP_403_FORBIDDEN

from performiq.api.urls.names import PerformIQPathName
from performiq.utils.constants import CSVFieldTypeEnum
from saas.urls.namespaces import Namespace
from utils.unittests.s3_mock import mock_s3
from utils.unittests.test_case import ExtendedAPITestCase


class MapCSVFieldsAPITestCase(ExtendedAPITestCase):

    empty_header_row = ["", "", ""]
    header_row = ["view rate", "imps.", "url", "views", "cpv", "cost", "cpm"]
    data_row = [34.4, 100203, "youtube.com/video/3asdf32", 43245, 0.024, 500, 3.43]

    def tearDown(self) -> None:
        for filename in ["csv_file.csv", "csv_file"]:
            try:
                os.remove(filename)
            except FileNotFoundError:
                pass

    def _get_url(self):
        return reverse(Namespace.PERFORMIQ + ":" + PerformIQPathName.MAP_CSV_FIELDS)

    def _create_csv(self, filename="csv_file.csv", write_header=True, write_data=True, write_empty_header=False):
        try:
            os.remove(filename)
        except FileNotFoundError:
            pass
        with open(filename, mode="w") as f:
            writer = csv.writer(f)
            if write_empty_header:
                writer.writerow(self.empty_header_row)
            if write_header:
                writer.writerow(self.header_row)
            if write_data:
                writer.writerow(self.data_row)
        return filename

    def test_permission(self):
        self.create_test_user()
        response = self.client.get(self._get_url())
        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)

    def test_bad_extension(self):
        self.create_admin_user()
        filename = self._create_csv("csv_file")
        with open(filename) as file:
            response = self.client.post(self._get_url(), {"csv_file": file})
            self.assertIn("The file extension must be '.csv'", response.json().get("csv_file")[0])
            self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_no_data(self):
        self.create_admin_user()
        filename = self._create_csv("csv_file.csv", write_header=False, write_data=False, write_empty_header=True)
        with open(filename) as file:
            response = self.client.post(self._get_url(), {"csv_file": file})
            self.assertIn("Header row invalid.", response.json().get("csv_file")[0])
            self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_missing_data_row(self):
        self.create_admin_user()
        filename = self._create_csv("csv_file.csv", write_data=False)
        with open(filename) as file:
            response = self.client.post(self._get_url(), {"csv_file": file})
            json = response.json()
            self.assertIn("one row besides the header row must be present", json.get("csv_file", [])[0])
            self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    @mock_s3
    def test_success(self):
        self.create_admin_user()
        conn = boto3.resource("s3", region_name="us-east-1")
        conn.create_bucket(Bucket=settings.AMAZON_S3_PERFORMIQ_CUSTOM_CAMPAIGN_UPLOADS_BUCKET_NAME)
        for write_header in [True, False]:
            with self.subTest(write_header):
                filename = self._create_csv("csv_file.csv", write_header=write_header)
                with open(filename) as file:
                    response = self.client.post(self._get_url(), {"csv_file": file})
                    self.assertEqual(response.status_code, HTTP_200_OK)
                    json = response.json()
                    self.assertIn("mapping", json)
                    self.assertIn("column_options", json)
                    self.assertIn("s3_key", json)
                    mapping = json.get("mapping", {})
                    # check keys always present
                    keys = mapping.keys()
                    valid_headers = [header.value for header in CSVFieldTypeEnum]
                    self.assertEqual(set(keys), set(valid_headers))
                    # check that letters are part of a set
                    mapping_values = list(filter(None, mapping.values()))
                    letters = list(string.ascii_uppercase)[:len(mapping_values)]
                    for value in mapping_values:
                        with self.subTest(value):
                            self.assertIn(value, letters)
                    # check column options depending on presence of header row
                    for value in json.get("column_options", {}).values():
                        with self.subTest(value):
                            if write_header:
                                self.assertIn(value, self.header_row)
                            else:
                                self.assertIn("column", value)

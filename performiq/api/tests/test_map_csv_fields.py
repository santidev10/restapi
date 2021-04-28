import csv
import os
import string
from random import shuffle

import boto3
from django.conf import settings
from django.core.files.uploadedfile import SimpleUploadedFile
from django.urls import reverse
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_400_BAD_REQUEST
from rest_framework.status import HTTP_403_FORBIDDEN

from performiq.api.urls.names import PerformIQPathName
from performiq.api.serializers.map_csv_fields_serializer import EXPECTED_CONTENT_TYPES
from performiq.utils.constants import CSVFieldTypeEnum
from performiq.utils.map_csv_fields import CSVWithHeader
from performiq.utils.map_csv_fields import CSVWithOnlyData
from performiq.utils.map_csv_fields import AutomaticPlacementsReport
from performiq.utils.map_csv_fields import ManagedPlacementsReport
from saas.urls.namespaces import Namespace
from utils.unittests.s3_mock import mock_s3
from utils.unittests.test_case import ExtendedAPITestCase


class MapCSVFieldsAPITestCase(ExtendedAPITestCase):

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

    def _create_csv(self, filename="csv_file.csv", write_header=True, write_data=True, header_row=[], encoding="utf-8",
                    delimiter=","):
        try:
            os.remove(filename)
        except FileNotFoundError:
            pass
        with open(filename, mode="w", encoding=encoding) as f:
            writer = csv.writer(f, delimiter=delimiter)
            if write_header:
                writer.writerow(header_row if header_row else self.header_row)
            if write_data:
                writer.writerow(self.data_row)
        return filename

    def test_permission(self):
        self.create_test_user()
        response = self.client.post(self._get_url())
        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)

    def test_bad_extension(self):
        self.create_admin_user()
        filename = self._create_csv("csv_file")
        with open(filename) as file:
            response = self.client.post(self._get_url(), {"csv_file": file})
            self.assertIn("The file extension must be '.csv'", response.json().get("csv_file")[0])
            self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_bad_content_type(self):
        self.create_admin_user()
        filename = self._create_csv("csv_file")
        with open(filename) as file:
            content = file.read().encode("utf_8")
            data = SimpleUploadedFile(name="csv_file.csv", content=content, content_type="asdf/asdf")
            response = self.client.post(self._get_url(), {"csv_file": data})
            self.assertIn("The file's content type (asdf/asdf) is not of the expected:",
                          response.json().get("csv_file")[0])
            self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_no_data(self):
        self.create_admin_user()
        header_row = ["", "", ""]
        filename = self._create_csv("csv_file.csv", write_data=False, header_row=header_row)
        with open(filename) as file:
            response = self.client.post(self._get_url(), {"csv_file": file})
            self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)
            json = response.json()
            type_error_map = {
                CSVWithOnlyData.get_type_string(): "CSV cannot have a header row",
                CSVWithHeader.get_type_string(): "CSV must have at least two rows",
                ManagedPlacementsReport.get_type_string(): "Placements reports must have at least 4 rows",
                AutomaticPlacementsReport.get_type_string(): "Placements reports must have at least 4 rows"
            }
            for csv_type, expected_error in type_error_map.items():
                with self.subTest((csv_type, expected_error)):
                    self.assertIn(expected_error, json.get("csv_file", {}).get(csv_type, []))

    def test_missing_data_row(self):
        self.create_admin_user()
        filename = self._create_csv("csv_file.csv", write_data=False)
        with open(filename) as file:
            response = self.client.post(self._get_url(), {"csv_file": file})
            json = response.json()
            csv_type_key = CSVWithHeader.get_type_string()
            self.assertIn("CSV must have at least two rows",
                          json.get("csv_file", {}).get(csv_type_key, []))
            self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    @mock_s3
    def test_more_columns_than_alphabet_letters_success(self):
        self.create_admin_user()
        conn = boto3.resource("s3", region_name="us-east-1")
        conn.create_bucket(Bucket=settings.AMAZON_S3_PERFORMIQ_CUSTOM_CAMPAIGN_UPLOADS_BUCKET_NAME)
        header_row = self.header_row
        header_row.extend(list(string.ascii_uppercase))
        filename = self._create_csv("csv_file.csv", header_row=header_row)
        with open(filename) as file:
            response = self.client.post(self._get_url(), {"csv_file": file})
            self.assertEqual(response.status_code, HTTP_200_OK)
            json = response.json()
            # check column options depending on presence of header row
            self.assertIn("column_options", json)
            column_options = json.get("column_options", {}).values()
            self.assertEqual(len(column_options), len(header_row))
            for header in header_row:
                with self.subTest(header):
                    self.assertIn(header, column_options)

    @mock_s3
    def test_success(self):
        self.create_admin_user()
        conn = boto3.resource("s3", region_name="us-east-1")
        conn.create_bucket(Bucket=settings.AMAZON_S3_PERFORMIQ_CUSTOM_CAMPAIGN_UPLOADS_BUCKET_NAME)
        expected_content_types = EXPECTED_CONTENT_TYPES.copy()
        for write_header in [True, False]:
            with self.subTest(write_header):
                for delimiter in [",", "\t"]:
                    with self.subTest(delimiter):
                        filename = self._create_csv("csv_file.csv", write_header=write_header, delimiter=delimiter)
                        with open(filename) as file:
                            content = file.read().encode("utf_8")
                            shuffle(expected_content_types)
                            content_type = expected_content_types[0]
                            csv_file = SimpleUploadedFile(name="csv_file.csv", content=content,
                                                          content_type=content_type)
                            response = self.client.post(self._get_url(), {"csv_file": csv_file})
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

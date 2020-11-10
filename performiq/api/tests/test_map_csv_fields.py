import os
import csv
import string
from utils.unittests.test_case import ExtendedAPITestCase
from django.urls import reverse
from saas.urls.namespaces import Namespace
from performiq.api.urls.names import PerformIQPathName
from performiq.utils.constants import CSVFieldTypeEnum
from rest_framework.status import HTTP_400_BAD_REQUEST
from rest_framework.status import HTTP_200_OK


class MapCSVFieldsAPITestCase(ExtendedAPITestCase):

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
                writer.writerow(["", "", ""])
            if write_header:
                writer.writerow(["view rate", "imps.", "url", "views", "cpv", "cost", "cpm"])
            if write_data:
                writer.writerow([34.4, 100203, "youtube.com/video/3asdf32", 43245, 0.024, 500, 3.43])
        return filename

    def test_bad_extension(self):
        self.create_admin_user()
        filename = self._create_csv("csv_file")
        with open(filename) as file:
            response = self.client.post(self._get_url(), {"csv_file": file})
            self.assertIn("The file extension must be '.csv'", response.json().get("csv_file")[0])
            self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_missing_header(self):
        self.create_admin_user()
        filename = self._create_csv("csv_file.csv", write_header=False)
        with open(filename) as file:
            response = self.client.post(self._get_url(), {"csv_file": file})
            self.assertIn("Header row invalid.", response.json().get("csv_file")[0])
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

    def test_success(self):
        self.create_admin_user()
        filename = self._create_csv("csv_file.csv")
        with open(filename) as file:
            response = self.client.post(self._get_url(), {"csv_file": file})
            self.assertEqual(response.status_code, HTTP_200_OK)
            json = response.json()
            self.assertIn("mapping", json)
            self.assertIn("column_options", json)
            mapping = json.get("mapping")
            # check keys always present
            keys = mapping.keys()
            valid_headers = [header.value for header in CSVFieldTypeEnum]
            self.assertEqual(set(keys), set(valid_headers))
            # check that letters are part of a set
            values = mapping.values()
            letters = list(string.ascii_uppercase)[:len(values)]
            for value in values:
                with self.subTest(value):
                    self.assertIn(value, letters)

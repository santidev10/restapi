from django.urls import reverse
from rest_framework.status import HTTP_200_OK, HTTP_400_BAD_REQUEST

from aw_reporting.models import GeoTarget
from utils.utittests.test_case import ExtendedAPITestCase


class DocumentsTestCase(ExtendedAPITestCase):

    def setUp(self):
        self.user = self.create_test_user()

    def test_success_xlsx(self):
        GeoTarget.objects.get_or_create(
            id=9004056,
            defaults=dict(
                name=10001,
                canonical_name="10001,New York,United States",
                country_code="US",
                target_type="Postal Code", status="Active",
            )
        )
        GeoTarget.objects.get_or_create(
            id=9016366,
            defaults=dict(
                name="46919",
                canonical_name="46919,Indiana,United States",
                country_code="US",
                target_type="Postal Code", status="Active",
            )
        )
        GeoTarget.objects.get_or_create(
            id=9016367,
            defaults=dict(
                name="46920",
                canonical_name="46920,Indiana,United States",
                country_code="US",
                target_type="Postal Code", status="Active",
            )
        )
        GeoTarget.objects.get_or_create(
            id=9016368,
            defaults=dict(
                name="46923",
                canonical_name="46923,Indiana,United States",
                country_code="US",
                target_type="Postal Code", status="Active",
            )
        )
        GeoTarget.objects.get_or_create(
            id=9016369,
            defaults=dict(
                name="46926",
                canonical_name="46926,Indiana,United States",
                country_code="US",
                target_type="Postal Code", status="Active",
            )
        )
        url = reverse(
            "aw_creation_urls:document_to_changes",
            args=("postal_codes",)
        )
        with open('aw_creation/fixtures/tests/example_zip_codes.xlsx',
                  'rb') as fp:
            response = self.client.post(url, {'file': fp},
                                        format='multipart')
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data['result']), 5)
        self.assertEqual(
            set(i['id'] for i in response.data['result']),
            {
                9004056,
                9016368,
                9016369,
                9016366,
                9016367,
            },
        )
        self.assertEqual(
            set(response.data['undefined']),
            {
                'Input',
                '123456789',
                'Output',
                '12345',
                '9016351',
            },
        )

    def test_error_xls(self):
        url = reverse(
            "aw_creation_urls:document_to_changes",
            args=("postal_codes",)
        )
        with open('aw_creation/fixtures/tests/example_zip_codes.xls', 'rb') as fp:
            response = self.client.post(url, {'file': fp}, format='multipart')

        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_success_csv_upload(self):
        GeoTarget.objects.get_or_create(
            id=9031988,
            defaults=dict(
                name=94513,
                canonical_name="94513,California,United States",
                country_code="US",
                target_type="Postal Code", status="Active",
            )
        )
        GeoTarget.objects.get_or_create(
            id=9032054,
            defaults=dict(
                name=94596,
                canonical_name="94596,California,United States",
                country_code="US",
                target_type="Postal Code", status="Active",
            )
        )
        url = reverse(
            "aw_creation_urls:document_to_changes",
            args=("postal_codes",)
        )
        with open('aw_creation/fixtures/tests/zip_codes.csv', 'rb') as fp:
            response = self.client.post(url, {'file': fp},
                                        format='multipart')

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(
            set(i['id'] for i in response.data['result']),
            {9031988, 9032054},
        )

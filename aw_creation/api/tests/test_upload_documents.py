from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK
from aw_reporting.models import GeoTarget
from saas.utils_tests import ExtendedAPITestCase


class DocumentsTestCase(ExtendedAPITestCase):

    def setUp(self):
        self.user = self.create_test_user()

    def test_success_upload(self):
        GeoTarget.objects.create(
            id=9004056,
            name=10001,
            canonical_name="10001,New York,United States",
            country_code="US",
            target_type="Postal Code", status="Active",
        )
        GeoTarget.objects.create(
            id=9016366,
            name="46919",
            canonical_name="46919,Indiana,United States",
            country_code="US",
            target_type="Postal Code", status="Active",
        )
        GeoTarget.objects.create(
            id=9016367,
            name="46920",
            canonical_name="46920,Indiana,United States",
            country_code="US",
            target_type="Postal Code", status="Active",
        )
        GeoTarget.objects.create(
            id=9016368,
            name="46921",
            canonical_name="46921,Indiana,United States",
            country_code="US",
            target_type="Postal Code", status="Active",
        )
        GeoTarget.objects.create(
            id=9016369,
            name="46922",
            canonical_name="46922,Indiana,United States",
            country_code="US",
            target_type="Postal Code", status="Active",
        )
        url = reverse(
            "aw_creation_urls:document_to_changes",
            args=("postal_codes",)
        )
        with open('aw_creation/fixtures/example_zip_codes.xlsx',
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

    def test_success_csv_upload(self):
        GeoTarget.objects.create(
            id=9031988,
            name=94513,
            canonical_name="94513,California,United States",
            country_code="US",
            target_type="Postal Code", status="Active",
        )
        GeoTarget.objects.create(
            id=9032054,
            name=94596,
            canonical_name="94596,California,United States",
            country_code="US",
            target_type="Postal Code", status="Active",
        )
        url = reverse(
            "aw_creation_urls:document_to_changes",
            args=("postal_codes",)
        )
        with open('aw_creation/fixtures/zip_codes.csv', 'rb') as fp:
            response = self.client.post(url, {'file': fp},
                                        format='multipart')

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(
            set(i['id'] for i in response.data['result']),
            {9031988, 9032054},
        )

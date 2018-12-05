from rest_framework.status import HTTP_200_OK
from rest_framework.test import APITestCase


class DocumentationApiTestCase(APITestCase):
    def test_swagger_success(self):
        response = self.client.get("/docs/swagger/")

        self.assertEqual(response.status_code, HTTP_200_OK)

    def test_redoc_success(self):
        response = self.client.get("/docs/redoc/")

        self.assertEqual(response.status_code, HTTP_200_OK)

    def test_swagger_json_success(self):
        response = self.client.get("/docs/swagger.json")

        self.assertEqual(response.status_code, HTTP_200_OK)

    def test_swagger_yaml_success(self):
        response = self.client.get("/docs/swagger.yaml")

        self.assertEqual(response.status_code, HTTP_200_OK)

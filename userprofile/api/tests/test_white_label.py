from io import BytesIO
import json
from unittest.mock import patch

from django.test import override_settings
from django.urls import reverse
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_400_BAD_REQUEST
from rest_framework.status import HTTP_403_FORBIDDEN

from saas.urls.namespaces import Namespace
from userprofile.constants import DEFAULT_DOMAIN
from userprofile.models import WhiteLabel
from utils.unittests.test_case import ExtendedAPITestCase


class WhiteLabelAPITestCase(ExtendedAPITestCase):
    _url = reverse(Namespace.USER_PROFILE + ":" + "white_label")

    def setUp(self):
        default, _ = WhiteLabel.objects.get_or_create(domain=DEFAULT_DOMAIN)
        default.config = dict(domain_name=DEFAULT_DOMAIN)
        default.save()

    def _create_whitelabel(self, domain, config):
        white_label = WhiteLabel.objects.create(domain=domain, config=config)
        return white_label

    def test_no_permissions_success(self):
        """ Should accept GET request with no params with no permissions """
        response = self.client.get(self._url)
        self.assertEqual(response.status_code, HTTP_200_OK)

    def test_rc(self):
        """ Should return default for rc.viewiq """
        domain_name = "rc.viewiq.com"
        with override_settings(ALLOWED_HOSTS=['*']):
            response = self.client.get(self._url, SERVER_NAME=domain_name)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["domain_name"], DEFAULT_DOMAIN)

    def test_subdomain_success(self):
        """ Should handle subdomain """
        sub_domain = "testsubdomain"
        server_name = sub_domain + ".viewiq.com"
        config = {
            "domain": sub_domain,
            "email": "testsubdomain@email.com"
        }
        self._create_whitelabel(sub_domain, config)
        with override_settings(ALLOWED_HOSTS=['*']):
            response = self.client.get(self._url, SERVER_NAME=server_name)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(config["domain"], response.data["domain"])
        self.assertEqual(config["email"], response.data["email"])

    def test_multiple_subdomain(self):
        """ Should handle multiple subdomains """
        sub_domain = "multiple.subdomain"
        server_name = sub_domain + ".viewiq.com"
        config = {
            "domain": sub_domain,
            "email": "multiple.subdomain.viewiq@email.com"
        }
        self._create_whitelabel(sub_domain, config)
        with override_settings(ALLOWED_HOSTS=['*']):
            response = self.client.get(self._url, SERVER_NAME=server_name)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(config["domain"], response.data["domain"])
        self.assertEqual(config["email"], response.data["email"])

    def test_get_all_permissions_fail(self):
        """ Reject GET requests for all configs without valid permissions """
        self.create_test_user()
        with override_settings(ALLOWED_HOSTS=['*']):
            response = self.client.get(self._url + "?all=true")
        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)

    def test_post_permissions_fail(self):
        """ Reject POST requests without valid permissions """
        self.create_test_user()
        with override_settings(ALLOWED_HOSTS=['*']):
            response = self.client.post(self._url)
        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)

    def test_patch_permissions_fail(self):
        """ Reject PATCH requests without valid permissions """
        self.create_test_user()
        with override_settings(ALLOWED_HOSTS=['*']):
            response = self.client.patch(self._url)
        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)

    def test_patch_success(self):
        self.create_admin_user()
        payload = {
            "domain": DEFAULT_DOMAIN,
            "logo": "test_url",
            "favicon": "test_favicon",
            "disable": ["google_oauth"],
        }
        with override_settings(ALLOWED_HOSTS=['*']):
            response = self.client.patch(self._url, json.dumps(payload), content_type="application/json")
        self.assertEqual(response.status_code, HTTP_200_OK)
        white_label = WhiteLabel.objects.get(domain=payload["domain"])
        self.assertEqual(response.data["domain"], white_label.config["domain"])
        self.assertEqual(response.data["logo"], white_label.config["logo"])
        self.assertEqual(response.data["favicon"], white_label.config["favicon"])
        self.assertEqual(response.data["disable"], white_label.config["disable"])

    def test_patch_invalid_fields_fail(self):
        self.create_admin_user()
        payload = {
            "domain": DEFAULT_DOMAIN,
            "invalid_field": "test_url",
        }
        with override_settings(ALLOWED_HOSTS=['*']):
            response = self.client.patch(self._url, json.dumps(payload), content_type="application/json")
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_post_image_success(self):
        self.create_admin_user()
        image_name = "test_logo_image.png"
        with override_settings(ALLOWED_HOSTS=['*']), \
                patch("userprofile.api.views.white_label.upload_file", return_value=image_name):
            image = BytesIO(b'mybinarydata')
            url = self._url + "?domain=viewiq&image_type=logo"
            response = self.client.post(url, image, content_type="image/png")
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["image_url"], image_name)

    def test_post_domain_success(self):
        self.create_admin_user()
        payload = {
            "domain": "test_new_domain",
            "logo": "test_logo_url"
        }
        with override_settings(ALLOWED_HOSTS=['*']):
            response = self.client.post(self._url, json.dumps(payload), content_type="application/json")
        self.assertEqual(response.status_code, HTTP_200_OK)
        white_label = WhiteLabel.objects.get(domain=payload["domain"])
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(payload["domain"], white_label.config["domain"])
        self.assertEqual(payload["logo"], white_label.config["logo"])

    def test_invalid_image_field_fail(self):
        self.create_admin_user()
        image_name = "test_logo_image.png"
        with override_settings(ALLOWED_HOSTS=['*']), \
                patch("userprofile.api.views.white_label.upload_file", return_value=image_name):
            image = BytesIO(b'mybinarydata')
            url = self._url + "?domain=viewiq&image_type=invalid_image_field"
            response = self.client.post(url, image, content_type="image/png")
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

import json
from io import BytesIO
from unittest.mock import patch

from django.test import override_settings
from django.urls import reverse
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_400_BAD_REQUEST
from rest_framework.status import HTTP_403_FORBIDDEN

from saas.urls.namespaces import Namespace
from userprofile.constants import DEFAULT_DOMAIN
from userprofile.constants import StaticPermissions
from userprofile.models import WhiteLabel
from utils.unittests.int_iterator import int_iterator
from utils.unittests.test_case import ExtendedAPITestCase


class WhiteLabelAPITestCase(ExtendedAPITestCase):
    _url = reverse(Namespace.USER_PROFILE + ":" + "white_label")

    @classmethod
    def setUpTestData(cls):
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

    def test_permissions_success(self):
        """ Should accept GET request for all with permissions """
        self.create_test_user(perms={
            StaticPermissions.DOMAIN_MANAGER__READ_ALL: True,
        })
        response = self.client.get(self._url + "?all=true")
        data = response.data
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertIsNotNone(data["domains"])
        self.assertIsNotNone(data["permissions"])

    def test_rc(self):
        """ GET Should return default for rc.viewiq """
        domain_name = "rc.viewiq.com"
        with override_settings(ALLOWED_HOSTS=["*"]):
            response = self.client.get(self._url, SERVER_NAME=domain_name)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["domain"], DEFAULT_DOMAIN)

    def test_subdomain_success(self):
        """ GET should handle subdomain """
        sub_domain = "testsubdomain"
        server_name = sub_domain + ".viewiq.com"
        config = {
            "email": "testsubdomain@email.com"
        }
        self._create_whitelabel(sub_domain, config)
        with override_settings(ALLOWED_HOSTS=["*"]):
            response = self.client.get(self._url, SERVER_NAME=server_name)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(sub_domain, response.data["domain"])
        self.assertEqual(config["email"], response.data["config"]["email"])

    def test_multiple_subdomain(self):
        """ Should handle multiple subdomains """
        sub_domain = "multiple.subdomain"
        server_name = sub_domain + ".viewiq.com"
        config = {
            "email": "multiple.subdomain.viewiq@email.com"
        }
        self._create_whitelabel(sub_domain, config)
        with override_settings(ALLOWED_HOSTS=["*"]):
            response = self.client.get(self._url, SERVER_NAME=server_name)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(sub_domain, response.data["domain"])
        self.assertEqual(config["email"], response.data["config"]["email"])

    def test_get_all_permissions_fail(self):
        """ Reject GET requests for all configs without valid permissions """
        self.create_test_user()
        with override_settings(ALLOWED_HOSTS=["*"]):
            response = self.client.get(self._url + "?all=true")
        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)

    def test_post_permissions_fail(self):
        """ Reject POST requests without valid permissions """
        self.create_test_user()
        with override_settings(ALLOWED_HOSTS=["*"]):
            response = self.client.post(self._url)
        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)

    def test_patch_permissions_fail(self):
        """ Reject PATCH requests without valid permissions """
        self.create_test_user()
        with override_settings(ALLOWED_HOSTS=["*"]):
            response = self.client.patch(self._url)
        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)

    def test_patch_success(self):
        self.create_admin_user()
        white_label = WhiteLabel.objects.get(domain=DEFAULT_DOMAIN)
        payload = {
            "id": white_label.id,
            "domain": white_label.domain,
            "config": {
                "logo": "test_url",
                "favicon": "test_favicon",
                "disable": ["google_oauth"],
            }
        }
        with override_settings(ALLOWED_HOSTS=["*"], DOMAIN_MANAGEMENT_PERMISSIONS=["google_oauth"]):
            response = self.client.patch(self._url, json.dumps(payload), content_type="application/json; charset=utf-8")
        white_label.refresh_from_db()
        config = payload["config"]
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(config["logo"], white_label.config["logo"])
        self.assertEqual(config["favicon"], white_label.config["favicon"])
        self.assertEqual(config["disable"], white_label.config["disable"])

    def test_patch_success_updates_config(self):
        self.create_admin_user()
        white_label = WhiteLabel.objects.get(domain=DEFAULT_DOMAIN)
        white_label.config.update({
            "favicon": "test_favicon",
            "disable": ["google_oauth"],
        })
        white_label.save()
        payload = {
            "id": white_label.id,
            "domain": white_label.domain,
            "config": {
                "logo": "test_url",
            }
        }
        with override_settings(ALLOWED_HOSTS=["*"]):
            response = self.client.patch(self._url, json.dumps(payload), content_type="application/json; charset=utf-8")
        white_label.refresh_from_db()
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(white_label.config["logo"], payload["config"]["logo"])
        self.assertEqual(white_label.config["favicon"], "test_favicon")
        self.assertEqual(white_label.config["disable"], ["google_oauth"])

    def test_patch_invalid_fields_fail(self):
        self.create_admin_user()
        white_label = WhiteLabel.objects.get(domain=DEFAULT_DOMAIN)
        payload = {
            "id": white_label.id,
            "config": {
                "invalid_field": "test_url",
            }
        }
        with override_settings(ALLOWED_HOSTS=["*"]):
            response = self.client.patch(self._url, json.dumps(payload), content_type="application/json; charset=utf-8")
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_patch_duplicate_domain_fail(self):
        self.create_admin_user()
        white_label_1 = WhiteLabel.objects.get(domain=DEFAULT_DOMAIN)
        white_label_2 = WhiteLabel.objects.create(domain=DEFAULT_DOMAIN + "2")
        payload = {
            "id": white_label_1.id,
            "domain": white_label_2.domain
        }
        with override_settings(ALLOWED_HOSTS=["*"]):
            response = self.client.patch(self._url, json.dumps(payload), content_type="application/json; charset=utf-8")
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_patch_invalid_disable_fail(self):
        """ Reject disable permissions that do not match """
        self.create_admin_user()
        white_label_1 = WhiteLabel.objects.get(domain=DEFAULT_DOMAIN)
        valid_permissions = ["valid_permission"]
        payload = {
            "id": white_label_1.id,
            "config": {
                "disable": [valid_permissions[0], "not a valid permission"]
            }
        }
        with override_settings(ALLOWED_HOSTS=["*"], DOMAIN_MANAGEMENT_PERMISSIONS=valid_permissions):
            response = self.client.patch(self._url, json.dumps(payload), content_type="application/json; charset=utf-8")
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_post_duplicate_domain_fail(self):
        self.create_admin_user()
        white_label = WhiteLabel.objects.get(domain=DEFAULT_DOMAIN)
        payload = {
            "domain": white_label.domain
        }
        with override_settings(ALLOWED_HOSTS=["*"]):
            response = self.client.post(self._url, json.dumps(payload), content_type="application/json; charset=utf-8")
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_post_default_disable(self):
        """ Disable all permissions for new domain if not provided"""
        self.create_admin_user()
        domain = f"test_domain_{next(int_iterator)}"
        permissions = ["perm_1", "perm_2", "perm_3"]
        payload = {
            "domain": domain
        }
        with override_settings(ALLOWED_HOSTS=["*"], DOMAIN_MANAGEMENT_PERMISSIONS=permissions):
            response = self.client.post(self._url, json.dumps(payload), content_type="application/json; charset=utf-8")
        white_label = WhiteLabel.objects.get(domain=domain)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(white_label.config["disable"], permissions)

    def test_post_image_success(self):
        self.create_admin_user()
        image_name = "test_logo_image.png"
        white_label = WhiteLabel.objects.get(domain=DEFAULT_DOMAIN)
        with override_settings(ALLOWED_HOSTS=["*"]), \
             patch("userprofile.api.views.white_label.upload_file", return_value=image_name):
            image = BytesIO(b"mybinarydata")
            url = self._url + f"?id={white_label.id}&image_type=logo"
            response = self.client.post(url, image, content_type="image/png")
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["image_url"], image_name)

    def test_post_domain_success(self):
        self.create_admin_user()
        payload = {
            "domain": "test_new_domain",
            "config": {
                "logo": "test_logo_url",
            }
        }
        with override_settings(ALLOWED_HOSTS=["*"]):
            response = self.client.post(self._url, json.dumps(payload), content_type="application/json; charset=utf-8")
        white_label = WhiteLabel.objects.get(domain=payload["domain"])
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(payload["domain"], white_label.domain)
        self.assertEqual(payload["config"]["logo"], white_label.config["logo"])

    def test_invalid_image_field_fail(self):
        self.create_admin_user()
        image_name = "test_logo_image.png"
        white_label = WhiteLabel.objects.get(domain=DEFAULT_DOMAIN)
        with override_settings(ALLOWED_HOSTS=["*"]), \
             patch("userprofile.api.views.white_label.upload_file", return_value=image_name):
            image = BytesIO(b"mybinarydata")
            url = self._url + f"?id={white_label.id}&image_type=invalid_image_field"
            response = self.client.post(url, image, content_type="image/png")
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_save_empty_domain_fail(self):
        self.create_admin_user()
        payload = {
            "domain": "",
        }
        with override_settings(ALLOWED_HOSTS=["*"]):
            response = self.client.post(self._url, json.dumps(payload), content_type="application/json; charset=utf-8")
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

        payload = {
            "domain": " ",
        }
        with override_settings(ALLOWED_HOSTS=["*"]):
            response = self.client.post(self._url, json.dumps(payload), content_type="application/json; charset=utf-8")
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

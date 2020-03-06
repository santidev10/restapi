from django.urls import reverse
from rest_framework.status import HTTP_200_OK

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
        """ Should accept request with no permissions """
        response = self.client.get(self._url)
        self.assertEqual(response.status_code, HTTP_200_OK)

    def test_rc(self):
        """ Should return default for rc.viewiq """
        domain_name = "rc.viewiq.com"
        response = self.client.get(self._url, SERVER_NAME=domain_name)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["domain_name"], DEFAULT_DOMAIN)

    def test_hyphen(self):
        """ Should ignore rc- for rc-* subdomains """
        domain_name = "rc-testdomain.viewiq"
        server_name = domain_name + ".com"
        config = {
            "sub_domain": domain_name,
            "email": "testdomain@email.com"
        }
        self._create_whitelabel("testdomain.viewiq", config)
        response = self.client.get(self._url, SERVER_NAME=server_name)
        self.assertEqual(config["sub_domain"], response.data["sub_domain"])
        self.assertEqual(config["email"], response.data["email"])

    def test_subdomain_success(self):
        """ Should handle subdomain """
        domain_name = "testsubdomain.viewiq"
        server_name = domain_name + ".com"
        config = {
            "sub_domain": domain_name,
            "email": "testsubdomain@email.com"
        }
        self._create_whitelabel("testsubdomain.viewiq", config)
        response = self.client.get(self._url, SERVER_NAME=server_name)
        self.assertEqual(config["sub_domain"], response.data["sub_domain"])
        self.assertEqual(config["email"], response.data["email"])

    def test_multiple_subdomain(self):
        """ Should handle multiple subdomains """
        domain_name = "multiple.subdomain.viewiq"
        server_name = domain_name + ".com"
        config = {
            "sub_domain": "multiple.subdomain.viewiq",
            "email": "multiple.subdomain.viewiq@email.com"
        }
        self._create_whitelabel(domain_name, config)
        response = self.client.get(self._url, SERVER_NAME=server_name)
        self.assertEqual(config["sub_domain"], response.data["sub_domain"])
        self.assertEqual(config["email"], response.data["email"])

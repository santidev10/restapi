from django.contrib.auth import get_user_model
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_400_BAD_REQUEST

from administration.api.urls.names import AdministrationPathName
from saas.urls.namespaces import Namespace
from userprofile.constants import UserStatuses
from utils.utittests.reverse import reverse
from utils.utittests.test_case import ExtendedAPITestCase


class AdminUpdateUserTestCase(ExtendedAPITestCase):
    def setUp(self):
        self.url = reverse(AdministrationPathName.USER_LIST, [Namespace.ADMIN])

    def test_success(self):
        expected_fields = {
            "id",
            "first_name",
            "last_name",
            "company",
            "phone_number",
            "email",
            "is_staff",
            "last_login",
            "date_joined",
            "token",
            "access",
            "google_account_id",
            "annual_ad_spend",
            "user_type",
            "status"
        }
        self.create_admin_user()
        get_user_model().objects.create(email="test_list@example.com")
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(set(response.data.get("items")[0].keys()), expected_fields)

    def test_wrong_order_by(self):
        self.create_admin_user()
        url = "{}{}".format(self.url, "?order_by=kgmeh")
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_ok_order_by(self):
        self.create_admin_user()
        user = get_user_model().objects.create(email="test_list1@example.com", first_name="A")
        get_user_model().objects.create(email="test_list2@example.com", first_name="B")
        url = "{}{}".format(self.url, "?order_by=first_name")
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data.get("items")[0].get("id"), user.id)

    def test_filter_by_status(self):
        self.create_admin_user()
        get_user_model().objects.create(email="test_list1@example.com", status=UserStatuses.ACTIVE.value)
        get_user_model().objects.create(email="test_list2@example.com", status=UserStatuses.REJECTED.value)
        url = "{}{}".format(self.url, "?status=active")
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data.get("items")), 1)

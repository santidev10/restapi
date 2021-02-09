from datetime import datetime

import pytz
from django.contrib.auth import get_user_model
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_400_BAD_REQUEST

from administration.api.serializers import UserSerializer
from administration.api.urls.names import AdministrationPathName
from saas.urls.namespaces import Namespace
from userprofile.models import Role
from userprofile.models import UserRole
from userprofile.constants import UserStatuses
from utils.unittests.reverse import reverse
from utils.unittests.test_case import ExtendedAPITestCase


class AdminUpdateUserTestCase(ExtendedAPITestCase):
    def setUp(self):
        self.url = reverse(AdministrationPathName.USER_LIST, [Namespace.ADMIN])

    def test_success(self):
        # pylint: disable=duplicate-code
        expected_fields = set(UserSerializer.Meta.fields)
        # pylint: enable=duplicate-code
        self.create_admin_user()
        get_user_model().objects.create(email="test_list@example.com")
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(set(response.data.get("items")[0].keys()), expected_fields)

    def test_wrong_sort_by(self):
        self.create_admin_user()
        url = "{}{}".format(self.url, "?sort_by=kgmeh")
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_char_field_sort_by_ascending(self):
        self.create_admin_user()
        get_user_model().objects.create(email="test_list1@example.com", first_name="B")
        user = get_user_model().objects.create(email="test_list2@example.com", first_name="A")
        url = "{}{}".format(self.url, "?sort_by=first_name&ascending=1")
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data.get("items")[0].get("id"), user.id)

    def test_char_field_sort_by_descending(self):
        self.create_admin_user()
        user = get_user_model().objects.create(email="test_list1@example.com", first_name="B")
        get_user_model().objects.create(email="test_list2@example.com", first_name="A")
        url = "{}{}".format(self.url, "?sort_by=first_name")
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data.get("items")[0].get("id"), user.id)

    def test_date_time_field_sort_by_ascending(self):
        self.create_admin_user()
        user1 = get_user_model().objects.create(email="test_list1@example.com")
        user2 = get_user_model().objects.create(email="test_list2@example.com")
        user1.date_joined = datetime(day=1, month=1, year=2018, hour=23, minute=23, tzinfo=pytz.utc)
        user1.save()
        user2.date_joined = datetime(day=2, month=1, year=2018, hour=21, minute=20, tzinfo=pytz.utc)
        user2.save()
        url = "{}{}".format(self.url, "?sort_by=date_joined&ascending=1")
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data.get("items")[0].get("id"), user1.id)

    def test_date_time_field_sort_by_descending(self):
        self.create_admin_user()
        user1 = get_user_model().objects.create(email="test_list1@example.com")
        user2 = get_user_model().objects.create(email="test_list2@example.com")
        user1.date_joined = datetime(day=1, month=1, year=2018, hour=23, minute=23, tzinfo=pytz.utc)
        user1.save()
        user2.date_joined = datetime(day=2, month=1, year=2018, hour=21, minute=20, tzinfo=pytz.utc)
        user2.save()
        url = "{}{}".format(self.url, "?sort_by=date_joined")
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data.get("items")[0].get("id"), user2.id)

    def test_ascending_invalid(self):
        self.create_admin_user()
        url = "{}{}".format(self.url, "?ascending=kgmeh")
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_filter_by_status(self):
        self.create_admin_user()
        get_user_model().objects.create(email="test_list1@example.com", status=UserStatuses.ACTIVE.value)
        get_user_model().objects.create(email="test_list2@example.com", status=UserStatuses.REJECTED.value)
        url = "{}{}".format(self.url, "?status=active")
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data.get("items")), 1)

    def test_filter_by_role(self):
        self.create_admin_user()
        role = Role.objects.create(name="test_role")
        user1 = get_user_model().objects.create(email="test_list1@example.com", status=UserStatuses.ACTIVE.value)
        get_user_model().objects.create(email="test_list2@example.com", status=UserStatuses.REJECTED.value)

        UserRole.objects.create(user=user1, role=role)
        url = "{}{}".format(self.url, f"?role={role.id}")
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data.get("items")), 1)
        self.assertEqual(response.data["items"][0]["id"], user1.id)

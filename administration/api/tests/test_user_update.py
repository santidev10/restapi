import json

from django.contrib.auth import get_user_model
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_201_CREATED
from rest_framework.status import HTTP_400_BAD_REQUEST
from rest_framework.test import APITestCase

from saas.urls.namespaces import Namespace
from administration.api.urls.names import AdministrationPathName
from userprofile.constants import UserTypeRegular
from userprofile.constants import UserAnnualAdSpend
from userprofile.constants import UserStatuses
from userprofile.models import UserProfile
from utils.utittests.generic_test import generic_test
from utils.utittests.reverse import reverse
from utils.utittests.test_case import ExtendedAPITestCase
from django.core import mail


class AdminUpdateUserTestCase(ExtendedAPITestCase):
    @generic_test([
        (status, (status.value,), dict())
        for status in UserStatuses
    ])
    def test_status_valid(self, status):
        self.create_admin_user()
        user = get_user_model().objects.create(email="test_status@example.com")
        update_url = reverse(AdministrationPathName.USER_DETAILS, [Namespace.ADMIN], args=(user.id,))
        self.assertTrue(UserStatuses.has_value(status))
        payload = {"status": status}
        response = self.client.put(update_url, data=payload)
        self.assertEqual(response.status_code, HTTP_200_OK)
        user.refresh_from_db()
        self.assertEqual(user.status, status)

    @generic_test([
        (status, (status.value,), dict())
        for status in (UserStatuses.PENDING, UserStatuses.REJECTED)
    ])
    def test_status_valid_deactivate_user(self, status):
        self.create_admin_user()
        user = get_user_model().objects.create(email="test_status@example.com")
        update_url = reverse(AdministrationPathName.USER_DETAILS, [Namespace.ADMIN], args=(user.id,))
        self.assertTrue(UserStatuses.has_value(status))
        payload = {"status": status}
        response = self.client.put(update_url, data=payload)
        self.assertEqual(response.status_code, HTTP_200_OK)
        user.refresh_from_db()
        self.assertFalse(user.is_active)

    def test_status_valid_activate_user(self):
        status = UserStatuses.ACTIVE.value
        self.create_admin_user()
        user = get_user_model().objects.create(email="test_status@example.com")
        update_url = reverse(AdministrationPathName.USER_DETAILS, [Namespace.ADMIN], args=(user.id,))
        self.assertTrue(UserStatuses.has_value(status))
        payload = {"status": status}
        response = self.client.put(update_url, data=payload)
        self.assertEqual(response.status_code, HTTP_200_OK)
        user.refresh_from_db()
        self.assertTrue(user.is_active)
        self.assertEqual(len(mail.outbox), 1)

    def test_status_not_allow_none(self):
        self.create_admin_user()
        user = get_user_model().objects.create(email="test_status@example.com")
        update_url = reverse(AdministrationPathName.USER_DETAILS, [Namespace.ADMIN], args=(user.id,))
        payload = {"status": None}
        response = self.client.put(update_url, data=payload)
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_status_invalid(self):
        self.create_admin_user()
        user = get_user_model().objects.create(email="test_status@example.com")
        update_url = reverse(AdministrationPathName.USER_DETAILS, [Namespace.ADMIN], args=(user.id,))
        payload = {"status": "Wrong Status"}
        response = self.client.put(update_url, data=payload)
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_status_not_allow_blank(self):
        self.create_admin_user()
        user = get_user_model().objects.create(email="test_status@example.com")
        update_url = reverse(AdministrationPathName.USER_DETAILS, [Namespace.ADMIN], args=(user.id,))
        payload = {"status": ""}
        response = self.client.put(update_url, data=payload)
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_partial_update(self):
        self.create_admin_user()
        user = get_user_model().objects.create(email="test_status@example.com")
        update_url = reverse(AdministrationPathName.USER_DETAILS, [Namespace.ADMIN], args=(user.id,))
        payload = {}
        response = self.client.put(update_url, data=payload)
        self.assertEqual(response.status_code, HTTP_200_OK)

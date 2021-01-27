import json

from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
from django.core import mail
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_400_BAD_REQUEST
from rest_framework.status import HTTP_403_FORBIDDEN

from administration.api.urls.names import AdministrationPathName
from saas.urls.namespaces import Namespace
from userprofile.constants import DEFAULT_DOMAIN
from userprofile.constants import UserStatuses
from userprofile.models import WhiteLabel
from utils.unittests.generic_test import generic_test
from utils.unittests.reverse import reverse
from utils.unittests.test_case import ExtendedAPITestCase


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

    def test_status_valid_activate_user_correct_link(self):
        get_user_model().objects.all().delete()
        domain, _ = WhiteLabel.objects.get_or_create(domain=DEFAULT_DOMAIN)
        sub_domain, _ = WhiteLabel.objects.get_or_create(domain="rc")

        status = UserStatuses.ACTIVE.value
        self.create_admin_user()
        user_1 = get_user_model().objects.create(email="test_status_1@example.com", domain=domain)
        user_2 = get_user_model().objects.create(email="test_status_2@example.com", domain=sub_domain)

        update_url_1 = reverse(AdministrationPathName.USER_DETAILS, [Namespace.ADMIN], args=(user_1.id,))
        update_url_2 = reverse(AdministrationPathName.USER_DETAILS, [Namespace.ADMIN], args=(user_2.id,))

        self.assertTrue(UserStatuses.has_value(status))
        payload = {"status": status}
        response_1 = self.client.put(update_url_1, data=payload)
        response_2 = self.client.put(update_url_2, data=payload)

        self.assertEqual(response_1.status_code, HTTP_200_OK)
        self.assertEqual(response_2.status_code, HTTP_200_OK)
        user_1.refresh_from_db()
        user_2.refresh_from_db()

        self.assertTrue(user_1.is_active)
        self.assertTrue(user_2.is_active)

        self.assertEqual(len(mail.outbox), 2)
        self.assertTrue(f"http://www.{DEFAULT_DOMAIN}.com/login" in mail.outbox[0].alternatives[0][0])
        self.assertTrue(f"http://{sub_domain}.{DEFAULT_DOMAIN}.com/login" in mail.outbox[1].alternatives[0][0])

    def test_status_not_allow_none(self):
        self.create_admin_user()
        user = get_user_model().objects.create(email="test_status@example.com")
        update_url = reverse(AdministrationPathName.USER_DETAILS, [Namespace.ADMIN], args=(user.id,))
        payload = {"status": None}
        response = self.client.put(update_url, data=json.dumps(payload), content_type="application/json")
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

    def test_status_change_from_active_to_active_no_email(self):
        self.create_admin_user()
        user = get_user_model().objects.create(email="test_status@example.com", status=UserStatuses.ACTIVE.value)
        update_url = reverse(AdministrationPathName.USER_DETAILS, [Namespace.ADMIN], args=(user.id,))
        payload = {"status": UserStatuses.ACTIVE.value}
        response = self.client.put(update_url, data=payload)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(mail.outbox), 0)

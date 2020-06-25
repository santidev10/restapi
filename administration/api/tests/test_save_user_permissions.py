import json

from django.contrib.auth import get_user_model
from django.urls import reverse
from rest_framework.status import HTTP_200_OK

from utils.unittests.test_case import ExtendedAPITestCase


class PermissionsAPITestCase(ExtendedAPITestCase):

    def test_users_list_media_buying_add_on(self):
        user = self.create_test_user()
        user.is_staff = True
        user.save()

        allowed_user = get_user_model().objects.create(
            email="mr_bond_james_bond@mail.kz"
        )

        allowed_user.add_custom_user_permission("view_media_buying")
        get_user_model().objects.create(
            email="an_ordinary_beggar@mail.ru"
        )
        url = reverse("admin_api_urls:user_list")
        response = self.client.get(url)

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data["items"]), 2)
        for user in response.data["items"]:
            if user["email"] == allowed_user.email:
                self.assertIs(user["can_access_media_buying"], True)
            else:
                self.assertIs(user["can_access_media_buying"], False)

    def test_update_media_buying_add_have_no_affect(self):
        user = self.create_test_user()
        user.is_staff = True
        user.save()

        test_user = get_user_model().objects.create(
            email="mr_bond_james_bond@mail.kz"
        )
        self.assertFalse(test_user.has_perm("userprofile,view_media_buying"))

        url = reverse("admin_api_urls:user_details", args=(test_user.id,))
        response = self.client.put(
            url, json.dumps(dict(can_access_media_buying=True)),
            content_type="application/json",
        )
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertIs(response.data["can_access_media_buying"], False)
        test_user.refresh_from_db()
        self.assertFalse(test_user.has_perm("userprofile,view_media_buying"))

    def test_update_send_email_to_the_user(self):
        user = self.create_test_user()
        user.is_staff = True
        user.save()

        test_user = get_user_model().objects.create(
            email="mr_bond_james_bond@mail.kz"
        )

        url = reverse("admin_api_urls:user_details", args=(test_user.id,))
        response = self.client.put(
            url, json.dumps(dict(can_access_media_buying=True)),
            content_type="application/json",
        )
        self.assertEqual(response.status_code, HTTP_200_OK)
        # email sending disabled according to SAAS-1895
        # self.assertEqual(len(mail.outbox), 1)

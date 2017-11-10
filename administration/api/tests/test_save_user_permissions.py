from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK
from saas.utils_tests import ExtendedAPITestCase
from django.contrib.auth import get_user_model
import json


class PermissionsAPITestCase(ExtendedAPITestCase):

    def test_users_list_media_buying_add_on(self):
        self.user = self.create_test_user()
        self.user.is_staff = True
        self.user.save()

        allowed_user = get_user_model().objects.create(
            email="mr_bond_james_bond@mail.kz", can_access_media_buying=True,
        )
        get_user_model().objects.create(
            email="an_ordinary_beggar@mail.ru", can_access_media_buying=False,
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

    def test_update_media_buying_add_on(self):
        self.user = self.create_test_user()
        self.user.is_staff = True
        self.user.save()

        test_user = get_user_model().objects.create(
            email="mr_bond_james_bond@mail.kz"
        )

        url = reverse("admin_api_urls:user_details", args=(test_user.id,))
        response = self.client.put(
            url, json.dumps(dict(can_access_media_buying=True)), content_type='application/json',
        )
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertIs(response.data["can_access_media_buying"], True)
        test_user.refresh_from_db()
        self.assertIs(test_user.can_access_media_buying, True)



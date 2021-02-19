import json

from django.contrib.auth import get_user_model
from rest_framework.status import HTTP_400_BAD_REQUEST
from rest_framework.status import HTTP_403_FORBIDDEN
from rest_framework.status import HTTP_200_OK

from saas.urls.namespaces import Namespace
from userprofile.models import UserRole
from userprofile.models import Role
from userprofile.models import PermissionItem
from userprofile.api.urls.names import UserprofilePathName
from utils.unittests.reverse import reverse
from utils.unittests.test_case import ExtendedAPITestCase
from userprofile.constants import StaticPermissions


class UserPermissionsManagement(ExtendedAPITestCase):
    def _get_url(self, user_id=None):
        params = {}
        if user_id:
            params["user_id"] = user_id
        return reverse(
            UserprofilePathName.MANAGE_PERMISSIONS,
            [Namespace.USER_PROFILE],
            query_params=params
        )

    def test_permissions_fail(self):
        self.create_test_user()
        response = self.client.get(self._get_url())
        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)

    def test_bad_request_user_id(self):
        """ Test user_id query param is required """
        self.create_admin_user()
        response = self.client.get(self._get_url())
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

        response = self.client.post(self._get_url(), {}, content_type="application/json")
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_bad_request_invalid_user(self):
        self.create_admin_user()
        response = self.client.get(self._get_url(user_id=0))
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

        response = self.client.post(self._get_url(user_id=0), {}, content_type="application/json")
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_bad_request_post(self):
        """ Test invalid post data raises ValidationError """
        user = self.create_admin_user()

        with self.subTest("Test updating with non existent permission"):
            invalid_permission_name = "non_exists"
            payload = json.dumps({
                "permissions": {
                    invalid_permission_name: False
                }
            })
            response = self.client.post(self._get_url(user_id=user.id), payload, content_type="application/json")
            self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)
            err = response.data[0]
            self.assertTrue("Invalid" in err and invalid_permission_name in err)

        with self.subTest("Test updating with invalid value"):
            payload = json.dumps({
                "permissions": {
                    StaticPermissions.RESEARCH__VETTING: None
                },
            })
            response = self.client.post(self._get_url(user_id=user.id), payload, content_type="application/json")
            self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)
            self.assertTrue("must be true or false" in response.data[0])

    def test_get_success(self):
        user = self.create_test_user()
        user.perms.update({StaticPermissions.USER_MANAGEMENT: True})
        user.save()
        response = self.client.get(self._get_url(user.id))
        data = response.data
        self.assertEqual(response.status_code, HTTP_200_OK)
        management_perm = [perm for perm in data["permissions"] if perm["perm"] == StaticPermissions.USER_MANAGEMENT][0]
        self.assertEqual(user.email, data["email"])
        self.assertEqual(management_perm["enabled"], True)
        self.assertTrue(len(data) > 0)

    def test_post_access_success(self):
        user = self.create_test_user()
        user.perms.update({StaticPermissions.USER_MANAGEMENT: True})
        user.save()
        payload = json.dumps({
            "permissions": {
                StaticPermissions.BUILD__CTL: True,
                StaticPermissions.RESEARCH: True,
            },
        })
        response = self.client.post(self._get_url(user.id), data=payload, content_type="application/json")
        self.assertEqual(response.status_code, HTTP_200_OK)

        user.refresh_from_db()
        self.assertEqual(user.perms[StaticPermissions.BUILD__CTL], True)
        self.assertEqual(user.perms[StaticPermissions.RESEARCH], True)
        # Should not be updated
        self.assertEqual(user.perms.get(StaticPermissions.BSTE), None)

    def test_change_admin(self):
        """ Test that user must be admin to manage admin permissions"""
        target = self.create_test_user(email="test2@email.com", perms={
            StaticPermissions.ADMIN: False,
        })
        user = self.create_test_user()

        with self.subTest("Test fails updating admin permission as non admin user"):
            user.perms.update({StaticPermissions.USER_MANAGEMENT: True})
            payload = json.dumps({
                "permissions": {
                    StaticPermissions.ADMIN: True,
                },
            })
            # user is not admin user and is trying to change target user admin status
            response = self.client.post(self._get_url(target.id), data=payload, content_type="application/json")
            self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)
            target.refresh_from_db()
            self.assertEqual(target.perms[StaticPermissions.ADMIN], False)

        with self.subTest("Test success updating admin permission as admin user"):
            user.perms.update({
                StaticPermissions.ADMIN: True,
            })
            user.save()
            # Successful since user now has admin permission
            response = self.client.post(self._get_url(target.id), data=payload, content_type="application/json")
            self.assertEqual(response.status_code, HTTP_200_OK)
            target.refresh_from_db()
            self.assertEqual(target.perms[StaticPermissions.ADMIN], True)

    def test_update_role(self):
        self.create_admin_user()
        target = get_user_model().objects.create(email="test2@email.com", perms={
            StaticPermissions.BUILD: False,
        })
        role = Role.objects.create(name="test_update_role")
        perm = PermissionItem.objects.get(permission=StaticPermissions.BUILD)
        role.permissions.add(perm)

        with self.subTest("Test adding user to role"):
            payload = json.dumps({
                "permissions": {},
                "role_id": role.id
            })
            response = self.client.post(self._get_url(target.id), data=payload, content_type="application/json")
            self.assertEqual(response.status_code, HTTP_200_OK)

            target.refresh_from_db()
            self.assertTrue(UserRole.objects.filter(user=target, role=role).exists())
            # Adding user to role should preserve original permissions
            self.assertFalse(target.perms[StaticPermissions.BUILD], False)

        with self.subTest("Test removing user from role"):
            payload = json.dumps({
                "permissions": {
                    StaticPermissions.BUILD: True,
                },
                "role_id": None
            })
            response = self.client.post(self._get_url(target.id), data=payload, content_type="application/json")
            self.assertEqual(response.status_code, HTTP_200_OK)

            target.refresh_from_db()
            self.assertIsNone(target.user_role.role)

import json

from django.contrib.auth import get_user_model
from django.urls import reverse
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_403_FORBIDDEN

from saas.urls.namespaces import Namespace
from userprofile.api.urls.names import UserprofilePathName
from userprofile.constants import StaticPermissions
from userprofile.models import PermissionItem
from userprofile.models import Role
from userprofile.models import UserRole
from utils.unittests.int_iterator import int_iterator
from utils.unittests.test_case import ExtendedAPITestCase


class UserRoleRetrieveUpdateAPITestCase(ExtendedAPITestCase):
    def _get_url(self, role_id):
        kwargs = dict(pk=role_id)
        url = reverse(Namespace.USER_PROFILE + ":" + UserprofilePathName.ROLE_RETRIEVE_UPDATE, kwargs=kwargs)
        return url

    def setUp(self):
        super().setUp()
        self.user = self.create_test_user(perms={StaticPermissions.USER_MANAGEMENT: True})

    def test_get_single_role_success(self):
        """ Test successfully retrieving permissions and users associated with role """
        permissions = PermissionItem.objects.all().order_by("id")[:10]
        role, _ = Role.objects.get_or_create(name=f"role_{next(int_iterator)}")
        role.permissions.add(*permissions)

        user1 = get_user_model().objects.create(email=f"tester@{next(int_iterator)}.com")
        UserRole.objects.create(user=self.user, role=role)
        UserRole.objects.create(user=user1, role=role)

        response = self.client.get(self._get_url(role.id))
        data = response.data
        role_permissions = role.permissions.all()
        enabled_permissions = [perm for perm in data["permissions"] if perm["enabled"] is True]

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual([p.id for p in role_permissions], [p["id"] for p in enabled_permissions])
        self.assertEqual([u["id"] for u in data["users"]], [self.user.id, user1.id])

    def test_get_permissions_fail(self):
        self.user.perms = {}
        self.user.save()
        response = self.client.get(self._get_url("1"))
        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)

    def test_role_name_update(self):
        """ Test updating role name """
        role = Role.objects.create(name="test")
        payload = dict(
            name="update",
            permissions={
                StaticPermissions.USER_MANAGEMENT: True
            }
        )
        response = self.client.patch(self._get_url(role.id), data=json.dumps(payload), content_type="application/json")
        role.refresh_from_db()
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(role.name, payload["name"])

    def test_remove_role_permission(self):
        """ Test removing permission from role """
        # Create role with permission and remove with request
        role = Role.objects.create(name="test")
        role.permissions.add(PermissionItem.objects.get(permission=StaticPermissions.USER_MANAGEMENT))
        payload = dict(
            name=role.name,
            permissions={
                StaticPermissions.USER_MANAGEMENT: False
            }
        )
        response = self.client.patch(self._get_url(role.id), data=json.dumps(payload), content_type="application/json")
        role.refresh_from_db()
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertFalse(role.permissions.filter(permission=StaticPermissions.USER_MANAGEMENT).exists())

    def test_add_role_permission(self):
        """ Test adding new permission to role """
        # Create role without adding permissions
        role = Role.objects.create(name="test")
        payload = dict(
            name=role.name,
            permissions={
                StaticPermissions.USER_MANAGEMENT: True,
            }
        )
        response = self.client.patch(self._get_url(role.id), data=json.dumps(payload), content_type="application/json")
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertTrue(role.permissions.filter(permission=StaticPermissions.USER_MANAGEMENT).exists())

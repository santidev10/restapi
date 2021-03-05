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

    def _get_payload(self, name, permissions, users):
        payload = dict(
            name=name,
            permissions=permissions,
            users=users,
        )
        return payload

    def setUp(self):
        super().setUp()
        self.user = self.create_test_user(perms={StaticPermissions.USER_MANAGEMENT: True})

    def test_get_role_base_permissions(self):
        """ Test that permissions are simply retrieved if role id kwarg is -1 """
        response = self.client.get(self._get_url(0))
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertTrue(len(response.data) > 0)

    def test_get_single_role_success(self):
        """ Test successfully retrieving permissions and users associated with role """
        permissions = PermissionItem.objects.all().order_by("id").values_list("permission", flat=True)[:10]
        role, _ = Role.objects.get_or_create(name=f"role_{next(int_iterator)}", permissions={
            perm: True for perm in permissions
        })

        user1 = get_user_model().objects.create(email=f"tester@{next(int_iterator)}.com")
        user2 = get_user_model().objects.create(email=f"tester@{next(int_iterator)}.com")
        UserRole.objects.create(user=user1, role=role)
        UserRole.objects.create(user=user2, role=role)

        response = self.client.get(self._get_url(role.id))
        self.assertEqual(response.status_code, HTTP_200_OK)

        data = response.data
        role.refresh_from_db()
        actual_permissions = {
            perm["perm"]: perm["enabled"]
            for perm in data["permissions"] if perm["perm"] in role.permissions
        }
        self.assertEqual(actual_permissions, role.permissions)
        self.assertEqual([u["id"] for u in data["users"]], [user1.id, user2.id])

    def test_get_permissions_fail(self):
        self.user.perms = {}
        self.user.save()
        response = self.client.get(self._get_url("1"))
        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)

    def test_role_name_update(self):
        """ Test updating role name """
        role = Role.objects.create(name="test")
        payload = self._get_payload("update", {StaticPermissions.USER_MANAGEMENT: True}, [self.user.id])
        response = self.client.patch(self._get_url(role.id), data=json.dumps(payload), content_type="application/json")
        self.assertEqual(response.status_code, HTTP_200_OK)

        role.refresh_from_db()
        self.assertEqual(role.name, payload["name"])

    def test_remove_role_permission(self):
        """ Test removing permission from role """
        # Create role with permission and remove with request
        role = Role.objects.create(name="test", permissions={StaticPermissions.USER_MANAGEMENT: True})
        payload = self._get_payload(role.name, {StaticPermissions.USER_MANAGEMENT: False}, [self.user.id])
        response = self.client.patch(self._get_url(role.id), data=json.dumps(payload), content_type="application/json")
        self.assertEqual(response.status_code, HTTP_200_OK)

        role.refresh_from_db()
        self.assertFalse(role.permissions[StaticPermissions.USER_MANAGEMENT])

    def test_add_role_permission(self):
        """ Test adding new permission to role """
        # Create role without adding permissions
        role = Role.objects.create(name="test")
        payload = self._get_payload(role.name, {StaticPermissions.USER_MANAGEMENT: True}, [self.user.id])
        response = self.client.patch(self._get_url(role.id), data=json.dumps(payload), content_type="application/json")
        self.assertEqual(response.status_code, HTTP_200_OK)
        role.refresh_from_db()
        self.assertTrue(role.permissions[StaticPermissions.USER_MANAGEMENT])

    def test_add_role_user(self):
        """ Test adding users to role. User role permissions should be checked if user is in a role """
        role = Role.objects.create(name="test")
        # role.permissions.add(PermissionItem.objects.get(permission=StaticPermissions.PERFORMIQ))

        user1 = get_user_model().objects.create(email=f"tester@{next(int_iterator)}.com")
        user2 = get_user_model().objects.create(email=f"tester@{next(int_iterator)}.com")

        payload = self._get_payload(role.name, {StaticPermissions.PERFORMIQ: True}, [user1.id, user2.id])
        response = self.client.patch(self._get_url(role.id), data=json.dumps(payload), content_type="application/json")

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(UserRole.objects.filter(user_id__in=[user1.id, user2.id]).count(), 2)

        # Check that has_permission utility method is using role permission
        self.assertTrue(
            user1.has_permission(StaticPermissions.PERFORMIQ)
            and user2.has_permission(StaticPermissions.PERFORMIQ)
        )
        # Permission should not be added directly to user permission
        self.assertFalse(
            user1.perms.get(StaticPermissions.PERFORMIQ) is True
            and user2.perms.get(StaticPermissions.PERFORMIQ) is True
        )

    def test_remove_role_user(self):
        """ Test removing user from role. User permissions should revert to personal permissions once removed """
        role = Role.objects.create(name="test", permissions={StaticPermissions.PERFORMIQ: True})
        # role.permissions.add(PermissionItem.objects.get(permission=StaticPermissions.PERFORMIQ))

        user1 = get_user_model().objects.create(email=f"tester@{next(int_iterator)}.com")
        user2 = get_user_model().objects.create(email=f"tester@{next(int_iterator)}.com")

        user_role1 = UserRole.objects.create(role=role, user=user1)
        user_role2 = UserRole.objects.create(role=role, user=user2)
        payload = self._get_payload(role.name, {StaticPermissions.PERFORMIQ: True}, [])
        response = self.client.patch(self._get_url(role.id), data=json.dumps(payload), content_type="application/json")

        self.assertEqual(response.status_code, HTTP_200_OK)
        user_role1.refresh_from_db()
        user_role2.refresh_from_db()
        self.assertTrue(user_role1.role is None and user_role2.role is None)
        # Check original permissions are not changed
        self.assertTrue(
            user1.perms.get(StaticPermissions.PERFORMIQ) is not True
            and user2.perms.get(StaticPermissions.PERFORMIQ) is not True
        )

    def test_update_role_existing(self):
        """ Test updating existing user roles that were part of a different role """
        role1 = Role.objects.create(name="test1", permissions={StaticPermissions.PERFORMIQ: True})
        role2 = Role.objects.create(name="test2", permissions={StaticPermissions.MANAGED_SERVICE: True})

        user1 = get_user_model().objects.create(email=f"tester@{next(int_iterator)}.com")
        user2 = get_user_model().objects.create(email=f"tester@{next(int_iterator)}.com")
        user3 = get_user_model().objects.create(email=f"tester@{next(int_iterator)}.com")

        user_role1 = UserRole.objects.create(role=role1, user=user1)
        user_role2 = UserRole.objects.create(role=role1, user=user2)
        user_role3 = UserRole.objects.create(role=role1, user=user3)
        payload = self._get_payload(role2.name, {StaticPermissions.PERFORMIQ: True}, [user2.id, user3.id])
        response = self.client.patch(self._get_url(role2.id), data=json.dumps(payload), content_type="application/json")
        self.assertEqual(response.status_code, HTTP_200_OK)
        user_role1.refresh_from_db()
        user_role2.refresh_from_db()
        user_role3.refresh_from_db()
        self.assertEqual(user_role1.role_id, role1.id)
        # Should update to from role1 to rol2
        self.assertEqual(user_role2.role_id, role2.id)
        self.assertEqual(user_role3.role_id, role2.id)

    def test_default_value(self):
        """ Test that if a permission is not in a role, the default value is used """
        role = Role.objects.create(name="test1")

        permission = PermissionItem.objects.get(permission=StaticPermissions.RESEARCH)
        response = self.client.get(self._get_url(role.id))
        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        serialized = [item for item in data["permissions"] if item["perm"] == permission.permission][0]
        self.assertEqual(serialized["enabled"], permission.default_value)

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


class UserRoleListCreateAPITestCase(ExtendedAPITestCase):
    url = reverse(Namespace.USER_PROFILE + ":" + UserprofilePathName.ROLE_LIST_CREATE)

    def setUp(self):
        super().setUp()
        self.user = self.create_test_user(perms={StaticPermissions.USER_MANAGEMENT: True})

    def test_get_success(self):
        permissions = PermissionItem.objects.all().values_list("permission", flat=True)
        roles = [Role(name=f"role_{next(int_iterator)}", permissions={permissions[0]: True}) for _ in range(2)]
        Role.objects.bulk_create(roles)

        response = self.client.get(self.url)
        data = response.data
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(data[0]["name"], roles[0].name)
        self.assertEqual(data[1]["name"], roles[1].name)

    def test_get_permission_fail(self):
        self.user.perms = {}
        self.user.save()
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)

    def test_create_role(self):
        payload = dict(
            name="test_role",
            permissions={
                StaticPermissions.RESEARCH: False,
                StaticPermissions.USER_MANAGEMENT: True,
                StaticPermissions.MANAGED_SERVICE: False,
            },
            users=[self.user.id],
        )
        response = self.client.post(self.url, data=json.dumps(payload), content_type="application/json")
        self.assertEqual(response.status_code, HTTP_200_OK)

        role = Role.objects.get(name=payload["name"])
        permissions = set(role.permissions.keys())

        self.assertTrue({StaticPermissions.RESEARCH, StaticPermissions.USER_MANAGEMENT}.issubset(permissions))
        self.assertTrue(UserRole.objects.filter(role=role, user=self.user).exists())
        actual = {}
        expected = {}
        for perm in payload["permissions"]:
            actual[perm] = role.permissions[perm]
            expected[perm] = payload["permissions"][perm]
        self.assertEqual(actual, expected)

    def test_create_role_update_existing(self):
        """ Test updating existing user roles that were part of a different role during creation """
        role1 = Role.objects.create(name="test1", permissions={StaticPermissions.PERFORMIQ: True})
        user1 = get_user_model().objects.create(email=f"tester@{next(int_iterator)}.com")
        user2 = get_user_model().objects.create(email=f"tester@{next(int_iterator)}.com")
        user3 = get_user_model().objects.create(email=f"tester@{next(int_iterator)}.com")

        user_role1 = UserRole.objects.create(role=role1, user=user1)
        user_role2 = UserRole.objects.create(role=role1, user=user2)
        user_role3 = UserRole.objects.create(role=role1, user=user3)
        payload = dict(
            name="test2",
            permissions={StaticPermissions.PERFORMIQ: True},
            users=[user2.id, user3.id]
        )
        response = self.client.post(self.url, data=json.dumps(payload), content_type="application/json")
        self.assertEqual(response.status_code, HTTP_200_OK)

        new_role = Role.objects.get(name=payload["name"])
        user_role1.refresh_from_db()
        user_role2.refresh_from_db()
        user_role3.refresh_from_db()
        self.assertEqual(user_role1.role_id, role1.id)
        # Should update to from role1 to rol2
        self.assertEqual(user_role2.role_id, new_role.id)
        self.assertEqual(user_role3.role_id, new_role.id)

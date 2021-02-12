import json

from django.urls import reverse
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_403_FORBIDDEN

from saas.urls.namespaces import Namespace
from userprofile.api.urls.names import UserprofilePathName
from userprofile.constants import StaticPermissions
from userprofile.models import PermissionItem
from userprofile.models import Role
from utils.unittests.int_iterator import int_iterator
from utils.unittests.test_case import ExtendedAPITestCase


class UserRoleListCreateAPITestCase(ExtendedAPITestCase):
    url = reverse(Namespace.USER_PROFILE + ":" + UserprofilePathName.ROLE_LIST_CREATE)

    def setUp(self):
        super().setUp()
        self.user = self.create_test_user(perms={StaticPermissions.USER_MANAGEMENT: True})

    def test_get_success(self):
        permissions = PermissionItem.objects.all()
        roles = [Role(name=f"role_{next(int_iterator)}") for _ in range(2)]
        Role.objects.bulk_create(roles)
        roles[0].permissions.add(permissions[0])
        roles[1].permissions.add(permissions[1])

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
                StaticPermissions.RESEARCH: True,
                StaticPermissions.USER_MANAGEMENT: True,
                StaticPermissions.MANAGED_SERVICE: False,
            }
        )
        response = self.client.post(self.url, data=json.dumps(payload), content_type="application/json")
        self.assertEqual(response.status_code, HTTP_200_OK)

        role = Role.objects.get(name=payload["name"])
        permissions = set(role.permissions.all().values_list("permission", flat=True))

        self.assertTrue({StaticPermissions.RESEARCH, StaticPermissions.USER_MANAGEMENT}.issubset(permissions))
        self.assertFalse(StaticPermissions.MANAGED_SERVICE in permissions)

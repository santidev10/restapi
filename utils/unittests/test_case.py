import logging
from functools import wraps

from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
from rest_framework.test import APITestCase

from userprofile.constants import StaticPermissions
from userprofile.models import UserDeviceToken
from userprofile.models import UserProfile
from userprofile.models import PermissionItem
from utils.unittests.patch_user_settings import patch_user_settings

logger = logging.getLogger(__name__)


class TestUserMixin:
    test_user_data = {
        "username": "TestUser",
        "first_name": "TestUser",
        "last_name": "TestUser",
        "email": "test@example.com",
        "password": "test",
    }

    def create_test_user(self, **kwargs) -> UserProfile:
        """
        Make test user
        """
        email = kwargs.get("email", self.test_user_data["email"])
        get_user_model().objects.filter(email=email).delete()
        user_data = {**self.test_user_data, **kwargs}
        user = get_user_model().objects.create(
            **user_data,
        )
        user.set_password(user.password)
        user.save()
        UserDeviceToken.objects.create(user=user)
        return user

    def create_admin_user(self, **kwargs):
        admin_perm = {StaticPermissions.ADMIN: True}
        return self.create_test_user(is_staff=True, is_superuser=True, perms=admin_perm, **kwargs)

    def fill_all_groups(self, user):
        all_perm_groups = Group.objects.values_list("name", flat=True)
        for perm_group in all_perm_groups:
            user.add_custom_user_group(perm_group)


def with_authorized(create_user_fn):
    @wraps(create_user_fn)
    def wrapper(test_case_instance, auth=True, **kwargs):
        user = create_user_fn(test_case_instance, **kwargs)
        if auth:
            device_token = UserDeviceToken.objects.create(user=user)
            test_case_instance.request_user = user
            test_case_instance.client.credentials(
                HTTP_AUTHORIZATION="Token {}".format(device_token.key)
            )
        return user

    return wrapper


class APITestUserMixin(TestUserMixin):
    def __init__(self):
        self.request_user = None

    @with_authorized
    def create_test_user(self, **kwargs):
        return super(APITestUserMixin, self).create_test_user(**kwargs)

    @with_authorized
    def create_admin_user(self, **kwargs):
        return super(APITestUserMixin, self).create_admin_user(**kwargs)


class ExtendedAPITestCase(APITestCase, APITestUserMixin):
    multi_db = True

    def patch_user_settings(self, **kwargs):
        return patch_user_settings(self.request_user, **kwargs)

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        PermissionItem.load_permissions()

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()
import json

from django.contrib.auth import get_user_model
from rest_framework.authtoken.models import Token
from rest_framework.test import APITestCase


class ExtendedAPITestCase(APITestCase):

    multi_db = True

    test_user_data = {
        "username": "TestUser",
        "first_name": "TestUser",
        "last_name": "TestUser",
        "email": "test@example.com",
        "password": "test"
    }

    def create_test_user(self, auth=True):
        """
        Make test user
        """
        user, created = get_user_model().objects.get_or_create(
            email=self.test_user_data["email"],
            defaults=self.test_user_data,
        )
        user.set_password(user.password)

        if auth:
            token = Token.objects.create(user=user)
            # pylint: disable=no-member
            self.client.credentials(
                HTTP_AUTHORIZATION='Token {}'.format(token.key)
            )
            # pylint: enable=no-member
        return user


class SingleDatabaseApiConnectorPatcher:
    """
    We can use the class to patch SingleDatabaseApiConnector in tests
    """

    @staticmethod
    def get_channel_list(*args, **kwargs):
        with open('saas/fixtures/singledb_channel_list.json') as data_file:
            data = json.load(data_file)
        return data

    @staticmethod
    def get_video_list(*args, **kwargs):
        with open('saas/fixtures/singledb_video_list.json') as data_file:
            data = json.load(data_file)
        return data

    def get_custom_query_result(self, *args, **kwargs):
        model_name = kwargs.get('model_name')
        if model_name == "video":
            items = self.get_video_list(*args, **kwargs).get("items")
        elif model_name == "channel":
            items = self.get_channel_list(*args, **kwargs).get("items")
        else:
            items = []
        return items[:kwargs.get('limit', 12)]

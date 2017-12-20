import json

from django.conf import settings
from django.contrib.auth import get_user_model
from rest_framework.authtoken.models import Token
from rest_framework.test import APITestCase

from userprofile.models import Plan


class ExtendedAPITestCase(APITestCase):
    multi_db = True

    test_user_data = {
        "username": "TestUser",
        "first_name": "TestUser",
        "last_name": "TestUser",
        "email": "test@example.com",
        "password": "test",
        "plan_id": settings.DEFAULT_ACCESS_PLAN_NAME,
    }

    def create_test_user(self, auth=True):
        """
        Make test user
        """
        Plan.update_defaults()
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
        for i in data["items"]:
            i["channel_id"] = i["id"]
        return data

    @staticmethod
    def get_video_list(*args, **kwargs):
        with open('saas/fixtures/singledb_video_list.json') as data_file:
            data = json.load(data_file)
        for i in data["items"]:
            i["video_id"] = i["id"]
        return data

    def get_channels_base_info(self, *args, **kwargs):
        data = self.get_channel_list()
        return data["items"]

    def get_videos_base_info(self, *args, **kwargs):
        data = self.get_video_list()
        return data["items"]

    def auth_channel(self, *args):
        return dict(channel_id="Chanel Id", access_token="Access Token")

    def put_channel(self, query_params, pk, data):
        channel = self.get_channel(query_params, pk=pk)
        channel.update(data)
        return channel

    def get_channel(self, query_params, pk):
        with open('saas/fixtures/singledb_channel_list.json') as data_file:
            channels = json.load(data_file)
        channel = next(filter(lambda c: c["id"] == pk, channels["items"]))
        channel.update(dict(is_owner=True))
        return channel

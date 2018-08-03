import csv
import io
import itertools
import json
import logging
from contextlib import contextmanager
from datetime import datetime, date
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
from django.db import transaction
from django.http.request import HttpRequest
from django.test import override_settings
from rest_framework.authtoken.models import Token
from rest_framework.status import HTTP_200_OK
from rest_framework.test import APITestCase

from singledb.connector import SingleDatabaseApiConnector
from userprofile.models import UserProfile
from userprofile.permissions import Permissions
from utils.datetime import Time
from utils.registry import registry

logger = logging.getLogger(__name__)


class TestUserMixin:
    test_user_data = {
        "username": "TestUser",
        "first_name": "TestUser",
        "last_name": "TestUser",
        "email": "test@example.com",
        "password": "test",
    }

    def create_test_user(self, auth=True) -> UserProfile:
        """
        Make test user
        """
        get_user_model().objects.filter(email=self.test_user_data["email"]) \
            .delete()
        Permissions.sync_groups()
        user = get_user_model().objects.create(
            **self.test_user_data,
        )
        user.set_password(user.password)
        user.save()

        if auth:
            Token.objects.get_or_create(user=user)
        return user

    def create_admin_user(self):
        user = self.create_test_user()
        user.is_staff = True
        user.is_superuser = True
        user.save()
        return user

    def fill_all_groups(self, user):
        all_perm_groups = Group.objects.values_list('name', flat=True)
        for perm_group in all_perm_groups:
            user.add_custom_user_group(perm_group)


class APITestUserMixin(TestUserMixin):
    def create_test_user(self, auth=True):
        user = super(APITestUserMixin, self).create_test_user(auth)
        if Token.objects.filter(user=user).exists():
            self.request_user = user
            self.client.credentials(
                HTTP_AUTHORIZATION='Token {}'.format(user.token)
            )
        return user


class ExtendedAPITestCase(APITestCase, APITestUserMixin):
    multi_db = True

    def patch_user_settings(self, **kwargs):
        return patch_user_settings(self.request_user, **kwargs)


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
        query_params = kwargs.get("query_params", dict())
        size = query_params.get("size", 1)
        if size == 0:
            data["max_page"] = None
        aggregations = query_params.get("aggregations", None)
        if aggregations is None:
            del data["aggregations"]
        return data

    def get_keyword_list(*args, **kwargs):
        with open('saas/fixtures/singledb_keyword_list.json') as data_file:
            data = json.load(data_file)
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
        return channel

    def get_video(self, query_params, pk):
        with open('saas/fixtures/singledb_video_list.json') as data_file:
            videos = json.load(data_file)
        video = next(filter(lambda c: c["id"] == pk, videos["items"]))
        return video

    def store_ids(self, query_params):
        pass




class MockResponse(object):
    def __init__(self, status_code=HTTP_200_OK, **kwargs):
        self.status_code = status_code
        self._json = kwargs.pop("json", None)

    def json(self):
        return self._json or dict()

    @property
    def text(self):
        return json.dumps(self.json())


class SegmentFunctionalityMixin(object):
    def create_segment_relations(self, relation_model, segment, related_ids):
        for related_id in related_ids:
            relation_model.objects.create(
                segment=segment, related_id=related_id)


class SingleDBMixin(object):
    def obtain_channels_ids(self, size=50):
        size = min(size, 50)
        connector = SingleDatabaseApiConnector()
        params = {"fields": "channel_id", "size": size}
        response = connector.get_channel_list(params)
        return {obj["channel_id"] for obj in response["items"]}

    def obtain_videos_data(self, fields=None, size=50):
        if fields is None:
            fields = "channel_id,video_id"
        size = min(size, 50)
        connector = SingleDatabaseApiConnector()
        params = {"fields": fields, "size": size}
        response = connector.get_video_list(params)
        return response


def test_instance_settings(**kwargs):
    data = kwargs

    def get_settings(key):
        return data.get(key)

    return get_settings


@contextmanager
def patch_user_settings(user, **kwargs):
    user_settings_backup = user.aw_settings
    user.aw_settings = {**user_settings_backup, **kwargs}
    user.save()
    yield
    user.aw_settings = user_settings_backup
    user.save()


@contextmanager
def patch_now(now):
    if type(now) == date:
        now = datetime.combine(now, datetime.min.time())
    with patch.object(Time, "now", return_value=now):
        yield


@contextmanager
def patch_registy_user(user, **kwargs):
    backup_user = registry._user
    registry._user = user
    yield
    registry._user = backup_user


class SettingDoesNotExist:
    pass


patch_settings = override_settings


def build_csv_byte_stream(headers, rows):
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=headers)
    for row in rows:
        clean_row = dict([(k, v) for k, v in row.items() if k in headers])
        writer.writerow(clean_row)
    output.seek(0)
    text_csv = output.getvalue()
    stream = io.BytesIO(text_csv.encode())
    return stream


def get_current_release():
    import subprocess
    import re
    try:
        branches = subprocess.check_output(["git", "branch", "--merged"]) \
            .decode("utf-8").split("\n")

        regexp = re.compile(r"release\/([\d.]+)")

        release_branches = filter(lambda b: regexp.search(b), branches)
        releases = [regexp.search(b).group(1) for b in release_branches]
        return sorted(releases, reverse=True)[0]
    except:
        return "0.0"


def generic_test(args_list, debug_indexes=None):
    """
    Generates subtest per each item in the args_list
    :param args_list: (msg: str, args: List, kwargs: Dict)
    :return:
    """
    if debug_indexes:
        logger.warning("Do not commit with `debug_indexes`. For debug purposes only")
        args_list = [args_list[index] for index in debug_indexes]

    def wrapper(fn):
        def wrapped_test_function(self):
            for msg, args, kwargs in args_list:
                with self.subTest(msg=msg, **kwargs), transaction.atomic():
                    sid = transaction.savepoint()
                    fn(self, *args, **kwargs)
                    transaction.savepoint_rollback(sid)

        return wrapped_test_function

    return wrapper


int_iterator = itertools.count(1, 1)

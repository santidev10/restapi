import io
import tempfile
from contextlib import contextmanager
from datetime import datetime

from PIL import Image
from django.conf import settings
from django.test import override_settings
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_401_UNAUTHORIZED
from rest_framework.status import HTTP_413_REQUEST_ENTITY_TOO_LARGE

from saas.urls.namespaces import Namespace
from userprofile.api.urls.names import UserprofilePathName
from utils.aws.s3 import get_s3_client
from utils.datetime import TIMESTAMP_FORMAT
from utils.utils_tests import ExtendedAPITestCase
from utils.utils_tests import mock_s3
from utils.utils_tests import patch_now
from utils.utils_tests import reverse

S3_BUCKET = settings.AMAZON_S3_BUCKET_NAME


class BaseAvatarTestCase(ExtendedAPITestCase):
    url = reverse(UserprofilePathName.AVATAR, [Namespace.USER_PROFILE])


class UserAvatarUploadTestCase(BaseAvatarTestCase):
    def _request(self, image):
        return self.client.post(self.url, image.read(), content_type="image/png")

    @contextmanager
    def temp_image(self):
        image = Image.new("RGB", (2, 2))
        tmp_file = tempfile.NamedTemporaryFile(suffix=".png")
        image.save(tmp_file)
        with open(tmp_file.name, "rb") as data:
            yield data

    def test_permissions(self):
        with self.temp_image()as image:
            response = self._request(image)
        self.assertEqual(response.status_code, HTTP_401_UNAUTHORIZED)

    @mock_s3
    def test_success(self):
        self.create_test_user()
        with self.temp_image() as image:
            response = self._request(image)
        self.assertEqual(response.status_code, HTTP_200_OK)

    @mock_s3
    def test_store_valid_profile_image_url(self):
        now = datetime(2018, 1, 1, 2, 3, 4)
        user = self.create_test_user()
        with patch_now(now), \
             self.temp_image() as image:
            response = self._request(image)

        self.assertEqual(response.status_code, HTTP_200_OK)
        expected_avatar_url = "https://{bucket}.s3.amazonaws.com/user/{user_id}/avatar.png?t={timestamp}".format(
            bucket=S3_BUCKET,
            user_id=user.id,
            timestamp=now.strftime(TIMESTAMP_FORMAT)
        )
        user.refresh_from_db()
        self.assertEqual(response.data, expected_avatar_url)
        self.assertEqual(user.profile_image_url, expected_avatar_url)

    @mock_s3
    def test_store_valid_image(self):
        now = datetime(2018, 1, 1, 2, 3, 4)
        user = self.create_test_user()
        with patch_now(now), \
             self.temp_image() as image:
            image_bytes = io.BytesIO(image.read())
            image.seek(0)
            response = self._request(image)

        self.assertEqual(response.status_code, HTTP_200_OK)
        expected_key = "user/{user_id}/avatar.png".format(
            user_id=user.id,
        )
        s3 = get_s3_client()
        s3_objects = s3.list_objects(Bucket=S3_BUCKET)["Contents"]
        self.assertEqual(len(s3_objects), 1)
        self.assertEqual(s3_objects[0]["Key"], expected_key)
        stored_object = s3.get_object(
            Bucket=S3_BUCKET,
            Key=expected_key,
        )

        stored_image = io.BytesIO(stored_object["Body"].read())
        self.assertEqual(stored_image.getvalue(), image_bytes.getvalue())

    @mock_s3
    def test_bad_request_on_too_large_file(self):
        self.create_test_user()
        test_avatar_size_limit = 1 / 1024 / 1024  # 1 byte
        with self.temp_image() as image, \
                override_settings(MAX_AVATAR_SIZE_MB=test_avatar_size_limit):
            response = self._request(image)
        self.assertEqual(response.status_code, HTTP_413_REQUEST_ENTITY_TOO_LARGE)


class UserAvatarDeleteTestCase(BaseAvatarTestCase):
    def _request(self):
        return self.client.delete(self.url)

    def test_permissions(self):
        response = self._request()
        self.assertEqual(response.status_code, HTTP_401_UNAUTHORIZED)

    def test_success(self):
        self.create_test_user()
        response = self._request()
        self.assertEqual(response.status_code, HTTP_200_OK)

    def test_set_none(self):
        user = self.create_test_user()
        user.profile_image_url = "test_url"
        user.save()

        self._request()
        user.refresh_from_db()
        self.assertIsNone(user.profile_image_url)

from unittest import mock

from django.core.urlresolvers import reverse
from django.test import override_settings
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_400_BAD_REQUEST

from saas.urls.namespaces import Namespace
from userprofile.api.tests.test_settings import CUSTOM_AUTH_FLAGS
from userprofile.api.urls.names import UserprofilePathName
from utils.utittests.test_case import ExtendedAPITestCase


class ApexUserCheckTestCase(ExtendedAPITestCase):
    _url = reverse(Namespace.USER_PROFILE + ":" + UserprofilePathName.USER_PROFILE)

    @override_settings(CUSTOM_AUTH_FLAGS=CUSTOM_AUTH_FLAGS)
    def test_success_apex_user_with_http_origin(self):
        """ Test success check for APEX client with correct HTTP_ORIGIN """
        test_email = "test.apex_user@testuser.com"
        self.create_test_user(email=test_email)

        response = self.client.get(self._url,
                                   content_type="application/json",
                                   HTTP_ORIGIN="http://localhost:8000")

        self.assertEqual(response.status_code, HTTP_200_OK)

    @override_settings(CUSTOM_AUTH_FLAGS=CUSTOM_AUTH_FLAGS)
    def test_success_apex_user_with_http_referer(self):
        """ Test success check for APEX client with correct HTTP_REFERER """
        test_email = "test.apex_user@testuser.com"
        self.create_test_user(email=test_email)

        response = self.client.get(self._url,
                                   content_type="application/json",
                                   HTTP_REFERER="http://localhost:8000")

        self.assertEqual(response.status_code, HTTP_200_OK)

    @override_settings(CUSTOM_AUTH_FLAGS=CUSTOM_AUTH_FLAGS)
    def test_success_not_apex_user(self):
        """ Test success check for not APEX client"""
        test_email = "test.user@testuser.com"
        self.create_test_user(email=test_email)

        response = self.client.get(self._url,
                                   content_type="application/json",
                                   HTTP_ORIGIN="http://localhost:8000")

        self.assertEqual(response.status_code, HTTP_200_OK)

    @override_settings(CUSTOM_AUTH_FLAGS=CUSTOM_AUTH_FLAGS)
    @override_settings(APEX_HOST="http://apex:8000")
    def test_error_apex_user_with_http_origin(self):
        """ Test error check for APEX client with invalid HTTP_ORIGIN """
        with mock.patch('userprofile.api.views.user_profile.UserProfileApiView.get') \
                as user_profile_view:

            test_email = "test.apex_user@testuser.com"
            self.create_test_user(email=test_email)

            response = self.client.get(self._url,
                                       content_type="application/json",
                                       HTTP_ORIGIN="http://localhost:8000")

            self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)
            self.assertEqual(user_profile_view.call_count, 0)

    @override_settings(CUSTOM_AUTH_FLAGS=CUSTOM_AUTH_FLAGS)
    @override_settings(APEX_HOST="http://apex:8000")
    def test_error_apex_user_with_http_referer(self):
        """ Test error check for APEX client with invalid HTTP_REFERER """
        with mock.patch('userprofile.api.views.user_profile.UserProfileApiView.get') \
                as user_profile_view:

            test_email = "test.apex_user@testuser.com"
            self.create_test_user(email=test_email)

            response = self.client.get(self._url,
                                       content_type="application/json",
                                       HTTP_REFERER="http://localhost:8000")

            self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)
            self.assertEqual(user_profile_view.call_count, 0)



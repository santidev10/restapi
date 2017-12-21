from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK, HTTP_403_FORBIDDEN
from saas.utils_tests import ExtendedAPITestCase, SingleDatabaseApiConnectorPatcher
from unittest.mock import patch


class ItemsFromIdsAPITestCase(ExtendedAPITestCase):

    def setUp(self):
        self.user = self.create_test_user()
        self.add_custom_user_permission(self.user, "view_media_buying")

    def test_success_fail_has_no_permission(self):
        self.remove_custom_user_permission(self.user, "view_media_buying")

        url = reverse("aw_creation_urls:targeting_items_search",
                      args=("video", "gangnam"))
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)

    def test_success_video(self):
        url = reverse("aw_creation_urls:targeting_items_search",
                      args=("video", "gangnam"))
        with patch("aw_creation.api.views.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertGreater(len(response.data), 0)
        self.assertEqual(set(response.data[0].keys()), {"id", "name", "thumbnail", "criteria"})

    def test_success_channel(self):
        url = reverse("aw_creation_urls:targeting_items_search",
                      args=("video", "smthing"))
        with patch("aw_creation.api.views.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertGreater(len(response.data), 0)
        self.assertEqual(set(response.data[0].keys()), {"id", "name", "thumbnail", "criteria"})

    def test_success_keyword(self):
        from keyword_tool.models import KeyWord
        for text in ("spam", "ham", "test", "batman"):
            KeyWord.objects.create(text=text)

        url = reverse("aw_creation_urls:targeting_items_search",
                      args=("keyword", "am"))
        with patch("aw_creation.api.views.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 3)
        self.assertEqual(set(response.data[0].keys()), {"name", "criteria"})
        self.assertEqual(response.data[0]["name"], "am")

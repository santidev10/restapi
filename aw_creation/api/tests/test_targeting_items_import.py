from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK, HTTP_403_FORBIDDEN
from aw_creation.models import *
from aw_reporting.models import Topic
from saas.utils_tests import ExtendedAPITestCase, \
    SingleDatabaseApiConnectorPatcher
from unittest.mock import patch


class TargetingImportTestCase(ExtendedAPITestCase):

    def setUp(self):
        self.user = self.create_test_user()
        self.add_custom_user_permission(self.user, "view_media_buying")

    def test_success_fail_has_no_permission(self):
        self.remove_custom_user_permission(self.user, "view_media_buying")

        topics = ((3, "Arts & Entertainment"),)
        for uid, name in topics:
            Topic.objects.get_or_create(id=uid, defaults={'name': name})

        url = reverse("aw_creation_urls:targeting_items_import",
                      args=(TargetingItem.TOPIC_TYPE,))
        with open('aw_creation/fixtures/topic_list_tool.csv',
                  'rb') as fp:
            response = self.client.post("{}?is_negative=1".format(url), {'file': fp},
                                        format='multipart')
        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)

    def test_import_topic(self):
        topics = (
            (3, "Arts & Entertainment"),
            (47, "Autos & Vehicles",)
        )
        for uid, name in topics:
            Topic.objects.get_or_create(id=uid, defaults={'name': name})

        url = reverse("aw_creation_urls:targeting_items_import",
                      args=(TargetingItem.TOPIC_TYPE,))
        with open('aw_creation/fixtures/topic_list_tool.csv',
                  'rb') as fp:
            response = self.client.post("{}?is_negative=1".format(url), {'file': fp},
                                        format='multipart')
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 2)

        for i in response.data:
            self.assertEqual(set(i.keys()), {"criteria", "name"})

    def test_import_keyword(self):
        url = reverse(
            "aw_creation_urls:targeting_items_import",
            args=(TargetingItem.KEYWORD_TYPE,),
        )
        with open('aw_creation/fixtures/keywords.csv', 'rb') as fp:
            response = self.client.post(url, {'file': fp},
                                        format='multipart')

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 3)
        for i in response.data:
            self.assertEqual(set(i.keys()), {"criteria", "name"})

    def test_import_channel(self):

        url = reverse(
            "aw_creation_urls:targeting_items_import",
            args=(TargetingItem.CHANNEL_TYPE,),
        )
        with patch("aw_creation.api.serializers.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher):
            with open('aw_creation/fixtures/import_channels_list.csv',
                      'rb') as fp:
                response = self.client.post(url, {'file': fp},
                                            format='multipart')
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 3)
        for i in response.data:
            self.assertEqual(set(i.keys()), {"criteria", "name", "id", "thumbnail"})

    def test_import_interest(self):
        from aw_reporting.models import Audience
        for i in range(3):
            Audience.objects.create(
                id=i * 10000, name="Interest#{}".format(i),
                type=Audience.IN_MARKET_TYPE,
            )

        url = reverse(
            "aw_creation_urls:targeting_items_import",
            args=(TargetingItem.INTEREST_TYPE,),
        )
        with open('aw_creation/fixtures/'
                  'import_topics_list.csv', 'rb') as fp:
            response = self.client.post(url, {'file': fp},
                                        format='multipart')

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 2)

        for i in response.data:
            self.assertEqual(set(i.keys()), {"criteria", "name"})

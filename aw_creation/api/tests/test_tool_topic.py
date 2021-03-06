from urllib.parse import urlencode

from django.urls import reverse
from rest_framework.status import HTTP_200_OK

from aw_reporting.models import Topic
from userprofile.constants import StaticPermissions
from utils.unittests.test_case import ExtendedAPITestCase


class TopicToolTestCase(ExtendedAPITestCase):

    def setUp(self):
        self.user = self.create_test_user(perms={
            StaticPermissions.MANAGED_SERVICE__EXPORT: True,
            StaticPermissions.MEDIA_BUYING: True,
        })

    def create_topic(self, name="Parent", uid=10000):
        parent = Topic.objects.create(id=uid, name=name)
        Topic.objects.create(id=uid + 1, name="Child#1", parent=parent)
        Topic.objects.create(id=uid + 2, name="Child#2", parent=parent)
        return parent

    def test_success_get(self):
        self.create_topic()

        # optimization_topic_tool
        url = reverse("aw_creation_urls:setup_topic_tool")
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)

        data = response.data
        self.assertGreaterEqual(len(data), 1)
        self.assertEqual(
            set(data[0].keys()),
            {
                "id",
                "name",
                "children",
            }
        )

    def test_export_list(self):
        self.create_topic("Parent#2")
        self.create_topic("Parent#1", uid=20000)

        url = reverse(
            "aw_creation_urls:setup_topic_tool_export",
        )
        url = "{}?{}".format(
            str(url),
            urlencode({"auth_token": self.user.tokens.first().key}),
        )
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        lines = list(response)
        self.assertGreaterEqual(len(lines), 7)

    def test_export_list_with_ids(self):
        parent_2 = self.create_topic("Parent#2")
        parent_1 = self.create_topic("Parent#1", uid=20000)
        children = parent_1.children.first()

        url = reverse(
            "aw_creation_urls:setup_topic_tool_export",
        )
        url = "{}?{}".format(
            str(url),
            urlencode(
                {
                    "auth_token": self.user.tokens.first().key,
                    "export_ids": "{},{}".format(parent_2.id, children.id)
                },
            ),
        )
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        lines = list(response)
        self.assertEqual(len(lines), 3)

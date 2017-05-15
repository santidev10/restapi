from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK, HTTP_403_FORBIDDEN
from aw_reporting.models import Audience
from saas.utils_tests import ExtendedAPITestCase
from urllib.parse import urlencode


class AudienceToolTestCase(ExtendedAPITestCase):

    def setUp(self):
        self.user = self.create_test_user()

    def create_audience(self, name="Parent", aud_type=None):
        aud_type = aud_type or Audience.AFFINITY_TYPE
        parent = Audience.objects.create(
            name=name, type=aud_type)
        Audience.objects.create(
            name="Child#1", parent=parent, type=aud_type)
        Audience.objects.create(
            name="Child#2", parent=parent, type=aud_type)
        return parent

    def test_success_get(self):
        parent = self.create_audience()
        self.create_audience(aud_type=Audience.CUSTOM_AFFINITY_TYPE)

        # creation_topic_tool
        url = reverse(
            "aw_creation_urls:optimization_audience_tool")
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)

        data = response.data
        self.assertEqual(len(data), 1)
        self.assertEqual(
            set(data[0].keys()),
            {
                'id',
                'name',
                'children',
                'type',
            }
        )
        self.assertEqual(data[0]['name'], parent.name)
        self.assertEqual(len(data[0]['children']), 2)

    def test_export_list(self):
        self.create_audience("Parent#2")
        self.create_audience("Parent#1")
        self.create_audience(aud_type=Audience.CUSTOM_AFFINITY_TYPE)

        url = reverse(
            "aw_creation_urls:optimization_audience_tool_export",
        )
        url = "{}?{}".format(
            str(url),
            urlencode({'auth_token': self.user.auth_token.key}),
        )
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        lines = list(response)
        self.assertEqual(len(lines), 7)

    def test_export_list_with_ids(self):
        parent_2 = self.create_audience("Parent#2")
        parent_1 = self.create_audience("Parent#1")
        children = parent_1.children.first()

        url = reverse(
            "aw_creation_urls:optimization_audience_tool_export",
        )
        url = "{}?{}".format(
            str(url),
            urlencode(
                {
                    'auth_token': self.user.auth_token.key,
                    'export_ids': "{},{}".format(parent_2.id, children.id)
                },
            ),
        )
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        lines = list(response)
        self.assertEqual(len(lines), 3)



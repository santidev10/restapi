from django.core.urlresolvers import reverse
from django.http import QueryDict
from rest_framework.status import HTTP_200_OK

from aw_creation.api.urls.names import Name
from aw_reporting.models import Topic
from saas.urls.namespaces import Namespace
from utils.utils_tests import ExtendedAPITestCase


class TopicToolTestCase(ExtendedAPITestCase):
    url = reverse(Namespace.AW_CREATION + ":" + Name.TOPIC_LIST)

    def setUp(self):
        self.user = self.create_test_user()

    def test_search_topics(self):
        topic_1 = Topic.objects.create(id=1, name="name 1")
        topic_2 = Topic.objects.create(id=2, name="name 2")
        topic_3 = Topic.objects.create(id=3, name="name 3")

        filters = QueryDict(mutable=True)
        filters.setlist("title",
                        [
                            topic_1.name,
                            topic_2.name,
                            "not existing {}".format(topic_3.name)
                        ])

        url = "{}?{}".format(self.url, filters.urlencode())

        response = self.client.get(url)

        self.assertEqual(response.status_code, HTTP_200_OK)
        ids = [t["id"] for t in response.data]
        self.assertEqual(len(ids), 2)
        self.assertEqual(set(ids), {topic_1.id, topic_2.id})

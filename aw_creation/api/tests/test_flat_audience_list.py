from django.core.urlresolvers import reverse
from django.http import QueryDict
from rest_framework.status import HTTP_200_OK

from aw_creation.api.urls.names import Name
from aw_reporting.models import Audience
from saas.urls.namespaces import Namespace
from utils.utittests.test_case import ExtendedAPITestCase


class AudienceFlatListTestCase(ExtendedAPITestCase):
    url = reverse(Namespace.AW_CREATION + ":" + Name.AUDIENCE_LIST_FLAT)

    def setUp(self):
        self.user = self.create_test_user()

    def test_search_topics(self):
        audience_1 = Audience.objects.create(id=1, name="name 1")
        audience_2 = Audience.objects.create(id=2, name="name 2")
        audience_3 = Audience.objects.create(id=3, name="name 3")

        filters = QueryDict(mutable=True)
        filters.setlist("title",
                        [
                            audience_1.name,
                            audience_2.name,
                            "not existing {}".format(audience_3.name)
                        ])

        url = "{}?{}".format(self.url, filters.urlencode())

        response = self.client.get(url)

        self.assertEqual(response.status_code, HTTP_200_OK)
        ids = [t["id"] for t in response.data]
        self.assertEqual(len(ids), 2)
        self.assertEqual(set(ids), {audience_1.id, audience_2.id})

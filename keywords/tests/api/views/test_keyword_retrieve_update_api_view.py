from unittest.mock import patch

from es_components.tests.utils import ESTestCase
from rest_framework.status import HTTP_200_OK

from aw_reporting.models import AdGroup
from aw_reporting.models import Campaign
from aw_reporting.models import KeywordStatistic
from es_components.constants import Sections
from es_components.managers import KeywordManager
from es_components.models.keyword import Keyword
from keywords.api.names import KeywordPathName
from saas.urls.namespaces import Namespace
from utils.unittests.int_iterator import int_iterator
from utils.unittests.reverse import reverse
from utils.unittests.test_case import ExtendedAPITestCase


class KeywordRetrieveUpdateApiViewTestCase(ExtendedAPITestCase, ESTestCase):
    def get_url(self, **kwargs):
        return reverse(KeywordPathName.KEYWORD_ITEM, [Namespace.KEYWORD], **kwargs)

    def test_get_keyword(self):
        keyword = "#tigerzindahai"
        self.create_test_user()
        url = self.get_url(args=(keyword,))
        with patch("es_components.managers.keyword.KeywordManager.model.get", return_value=Keyword(id="keyword")):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)

    def test_no_aw_stats(self):
        self.create_admin_user()
        keyword = Keyword(id=str(next(int_iterator)))
        KeywordManager([Sections.STATS]).upsert([keyword])
        campaign = Campaign.objects.create()
        ad_group = AdGroup.objects.create(campaign=campaign)
        KeywordStatistic.objects.create(date="2020-01-01", cost=1, keyword=keyword.main.id, ad_group=ad_group)

        response = self.client.get(self.get_url(args=(keyword.main.id,)))

        self.assertNotIn("aw_stats", response.data)

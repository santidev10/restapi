from aw_reporting.models import AdGroup
from aw_reporting.models import Campaign
from aw_reporting.models import KeywordStatistic
from es_components.constants import Sections
from es_components.managers import KeywordManager
from es_components.models import Keyword
from es_components.tests.utils import ESTestCase
from keywords.api.names import KeywordPathName
from saas.urls.namespaces import Namespace
from utils.utittests.int_iterator import int_iterator
from utils.utittests.reverse import reverse
from utils.utittests.test_case import ExtendedAPITestCase


class KeywordListApiViewTestCase(ExtendedAPITestCase, ESTestCase):
    def get_url(self, **query_params):
        return reverse(KeywordPathName.KEYWORD_LIST, [Namespace.KEYWORD], query_params=query_params)

    def test_no_aw_stats(self):
        self.create_admin_user()
        keyword = Keyword(id=str(next(int_iterator)))
        KeywordManager([Sections.STATS]).upsert([keyword])
        campaign = Campaign.objects.create()
        ad_group = AdGroup.objects.create(campaign=campaign)
        KeywordStatistic.objects.create(date="2020-01-01", cost=1, keyword=keyword.main.id, ad_group=ad_group)

        response = self.client.get(self.get_url())

        self.assertNotIn("aw_stats", response.data["items"][0])

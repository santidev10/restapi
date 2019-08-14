from django.test import override_settings
from mock import patch
from rest_framework.status import HTTP_200_OK

from aw_reporting.models import AdGroup
from aw_reporting.models import Campaign
from aw_reporting.models import KeywordStatistic
from es_components.constants import Sections
from es_components.managers import ChannelManager
from es_components.managers import KeywordManager
from es_components.models import Channel
from es_components.models import Keyword
from es_components.tests.utils import ESTestCase
from keywords.api.names import KeywordPathName
from saas.urls.namespaces import Namespace
from utils.utittests.es_components_patcher import SearchDSLPatcher
from utils.utittests.int_iterator import int_iterator
from utils.utittests.reverse import reverse
from utils.utittests.test_case import ExtendedAPITestCase


class KeywordListApiViewTestCase(ExtendedAPITestCase, ESTestCase):
    def get_url(self, **query_params):
        return reverse(KeywordPathName.KEYWORD_LIST, [Namespace.KEYWORD], query_params=query_params)

    def test_get_keywords(self):
        url = self.get_url()

        self.create_admin_user()
        with patch("es_components.managers.keyword.KeywordManager.search", return_value=SearchDSLPatcher()):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)

    def test_no_aw_stats(self):
        self.create_admin_user()
        keyword = Keyword(id=str(next(int_iterator)))
        KeywordManager([Sections.STATS]).upsert([keyword])
        campaign = Campaign.objects.create()
        ad_group = AdGroup.objects.create(campaign=campaign)
        KeywordStatistic.objects.create(date="2020-01-01", cost=1, keyword=keyword.main.id, ad_group=ad_group)

        response = self.client.get(self.get_url())

        self.assertNotIn("aw_stats", response.data["items"][0])

    def test_works_with_cache(self):
        """
        Ticket: https://channelfactory.atlassian.net/browse/VIQ-2284
        Summary: Channels/Videos > 500 server error appears when user switch to the Tags tab
        """
        self.create_admin_user()
        channel = Channel(str(next(int_iterator)))
        channel.populate_general_data(video_tags=["test_tag"])
        ChannelManager(Sections.GENERAL_DATA).upsert([channel])

        url = self.get_url(from_channel=channel.main.id)
        with override_settings(ES_CACHE_ENABLED=True):
            response = self.client.get(url)

            self.assertEqual(HTTP_200_OK, response.status_code)

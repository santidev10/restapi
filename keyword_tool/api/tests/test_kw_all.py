from django.core.urlresolvers import reverse
from django.utils import timezone
from datetime import timedelta
from rest_framework.status import HTTP_200_OK
from aw_reporting.models import Account, Campaign, AdGroup, KeywordStatistic
from saas.utils_tests import ExtendedAPITestCase as APITestCase
from keyword_tool.models import *


class KWToolAPITestCase(APITestCase):

    def setUp(self):
        self.user = self.create_test_user()

    def test_get_all_keywords_with_cf_data(self):
        texts = ("Sol", "Tir", "Amn", "Ith")
        KeyWord.objects.bulk_create([KeyWord(text=t) for t in texts])

        account = Account.objects.create(id="1", name="CF customer account")
        manager = Account.objects.create(id="3386233102", name="Promopushmaster")
        account.managers.add(manager)
        campaign = Campaign.objects.create(id=1, name="", account=account)
        ad_group = AdGroup.objects.create(id=1, name="", campaign=campaign)
        today = timezone.now()
        for keyword in texts:
            KeywordStatistic.objects.create(date=today - timedelta(days=1), ad_group=ad_group, keyword=keyword,
                                            impressions=10, video_views=1)
            KeywordStatistic.objects.create(date=today, ad_group=ad_group, keyword=keyword,
                                            impressions=10, video_views=5)
            KeywordStatistic.objects.create(date=today + timedelta(days=1), ad_group=ad_group, keyword=keyword)

        url = reverse("keyword_tool_urls:kw_tool_all")
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data['items']), len(texts))

        for i in response.data['items']:
            self.assertEqual(i['impressions'], 20)
            self.assertEqual(i['video_view_rate_bottom'], 10)
            self.assertEqual(i['video_view_rate_top'], 50)


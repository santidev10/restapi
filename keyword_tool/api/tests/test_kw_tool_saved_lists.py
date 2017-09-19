from django.core.urlresolvers import reverse
from django.utils import timezone
from rest_framework.status import HTTP_200_OK
from aw_reporting.models import Account, Campaign, AdGroup, KeywordStatistic
from aw_reporting.api.tests.base import AwReportingAPITestCase
from keyword_tool.models import *


class KWToolAPITestCase(AwReportingAPITestCase):

    def setUp(self):
        self.user = self.create_test_user()

    def test_get_lists_with_users_data(self):

        texts = ("Sol", "Tir", "Amn", "Ith")
        kw_list = KeywordsList.objects.create(name="..", user_email=self.user.email)
        for text in texts:
            kw = KeyWord.objects.create(text=text)
            kw_list.keywords.add(kw)

        account = self.create_account(self.user)
        campaign = Campaign.objects.create(id=1, name="", account=account)
        ad_group = AdGroup.objects.create(id=1, name="", campaign=campaign)
        today = timezone.now()
        for keyword in texts:
            KeywordStatistic.objects.create(date=today, ad_group=ad_group, keyword=keyword, cost=1, video_views=2)

        url = reverse("keyword_tool_urls:kw_tool_saved_lists")
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data['items']), 1)

        for i in response.data['items']:
            self.assertEqual(i['average_cpv'], .5)

    def test_get_lists_with_cf_data(self):
        texts = ("Sol", "Tir", "Amn", "Ith")
        kw_list = KeywordsList.objects.create(name="..", user_email=self.user.email)
        for text in texts:
            kw = KeyWord.objects.create(text=text)
            kw_list.keywords.add(kw)

        account = Account.objects.create(id="1", name="CF customer account")
        manager = Account.objects.create(id="3386233102", name="Promopushmaster")
        account.managers.add(manager)
        campaign = Campaign.objects.create(id=1, name="", account=account)
        ad_group = AdGroup.objects.create(id=1, name="", campaign=campaign)
        today = timezone.now()
        for keyword in texts:
            KeywordStatistic.objects.create(date=today, ad_group=ad_group, keyword=keyword, cost=1, video_views=2)

        url = reverse("keyword_tool_urls:kw_tool_saved_lists")
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data['items']), 1)

        for i in response.data['items']:
            self.assertEqual(i['average_cpv'], .5)


from datetime import datetime
from unittest.mock import patch

from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_202_ACCEPTED
from rest_framework.status import HTTP_400_BAD_REQUEST

from aw_reporting.api.tests.base import AwReportingAPITestCase
from aw_reporting.models import AdGroup
from aw_reporting.models import Campaign
from aw_reporting.models import KeywordStatistic
from keyword_tool.models import *


class KWToolSavedListTestCase(AwReportingAPITestCase):
    def setUp(self):
        self.user = self.create_test_user()

    def test_get_saved_lists(self):

        my_list = KeywordsList.objects.create(
            user_email=self.user.email,
            name="My list",
            category="private"
        )
        KeywordsList.objects.create(
            user_email="donald.trump@mail.kz",
            name="Another list",
            category="private"
        )
        keywords = ("batman", "pepsi", "kek")
        account = self.create_account(self.user)
        campaign = Campaign.objects.create(id=1, name="", account=account)
        ad_group = AdGroup.objects.create(id=1, name="", campaign=campaign)
        date = datetime.now()
        for n, kw in enumerate(keywords):
            obj = KeyWord.objects.create(text=kw)
            my_list.keywords.add(obj)
            KeywordStatistic.objects.create(
                keyword=kw, ad_group=ad_group, date=date,
                impressions=6, clicks=n, video_views=n + 1, cost=2,
            )

        url = reverse("keyword_tool_urls:kw_tool_saved_lists")
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data['items']), 1)
        returned_list = response.data['items'][0]
        self.assertEqual(
            set(returned_list.keys()),
            {
                'id', 'name',
                'category',
                'average_cpc',
                'is_editable',
                'average_volume',
                'num_keywords',
                'top_keywords_data',
                'competition',
                'owner',
                'created_at',
                'is_owner',

                'average_cpv',
                'video_view_rate',
                'ctr_v',
            }
        )
        self.assertEqual(returned_list['id'], my_list.id)
        self.assertEqual(returned_list['name'], my_list.name)
        self.assertEqual(returned_list['average_cpv'], 1)
        self.assertAlmostEqual(returned_list['video_view_rate'], 33.33, 2)
        self.assertEqual(returned_list['ctr_v'], 50)

    def test_fail_create_chf_saved_lists(self):
        keywords = []
        for i in range(1, 4):
            keyword = KeyWord.objects.create(text="kw #%s" % i)
            keywords.append(keyword.text)

        url = reverse("keyword_tool_urls:kw_tool_saved_lists")
        response = self.client.post(
            url,
            json.dumps(dict(
                name="My list",
                keywords=keywords,
                category='chf',
            )),
            content_type="application/json"
        )
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)
        self.assertIn('category', response.data)

    def test_create_saved_lists(self):
        name = "My list"
        keywords = []
        category = 'private'
        for i in range(1, 10):
            keyword = KeyWord.objects.create(text="kw #%s" % i)
            keywords.append(keyword.text)

        url = reverse("keyword_tool_urls:kw_tool_saved_lists")
        with patch("keyword_tool.api.views.update_kw_list_stats") as patched_task:
            response = self.client.post(
                url,
                json.dumps(dict(
                    name=name,
                    keywords=keywords,
                    category=category,
                )),
                content_type="application/json"
            )
            self.assertEqual(response.status_code, HTTP_202_ACCEPTED)
            self.assertEqual(response.data['name'], name)
            self.assertEqual(response.data['category'], category)
            self.assertEqual(patched_task.delay.call_count, 1)

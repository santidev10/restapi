from django.contrib.auth import get_user_model
from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK, HTTP_202_ACCEPTED, HTTP_404_NOT_FOUND, HTTP_400_BAD_REQUEST
from keyword_tool.models import *
from unittest.mock import patch
from aw_reporting.api.tests.base import AwReportingAPITestCase
from aw_reporting.models import Campaign, AdGroup, KeywordStatistic
from datetime import datetime


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
                'cum_average_volume_per_kw_data',
                'is_editable',
                'cum_average_volume_data',
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
            dict(
                name="My list",
                keywords=keywords,
                category='chf',
            )
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
                dict(
                    name=name,
                    keywords=keywords,
                    category=category,
                )
            )
            self.assertEqual(response.status_code, HTTP_202_ACCEPTED)
            self.assertEqual(response.data['name'], name)
            self.assertEqual(response.data['category'], category)
            self.assertEqual(patched_task.delay.call_count, 1)

    def test_get_saved_list(self):
        saved_list = KeywordsList.objects.create(
            user_email=self.user.email,
            name="My list",
            category="private"
        )
        url = reverse(
            "keyword_tool_urls:kw_tool_saved_list",
            args=(saved_list.id,),
        )
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(
            set(response.data.keys()),
            {
                'id', 'name',
                'category',
                'average_cpc',
                'cum_average_volume_per_kw_data',
                'is_editable',
                'cum_average_volume_data',
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

    def test_update_saved_list(self):
        saved_list = KeywordsList.objects.create(
            user_email=self.user.email,
            name="My list",
            category="private"
        )

        url = reverse(
            "keyword_tool_urls:kw_tool_saved_list",
            args=(saved_list.id,),
        )
        another_user = get_user_model().objects.create(
            email="donald.trump@mail.kz",
        )
        updates = dict(
            name="New name"
        )
        response = self.client.put(url, updates)
        self.assertEqual(response.status_code, HTTP_202_ACCEPTED)
        self.assertEqual(response.data['name'], updates['name'])

    def test_delete_saved_list(self):
        saved_list = KeywordsList.objects.create(
            user_email=self.user.email,
            name="My list",
            category="private"
        )
        url = reverse(
            "keyword_tool_urls:kw_tool_saved_list",
            args=(saved_list.id,),
        )
        response = self.client.delete(url)
        self.assertEqual(response.status_code, HTTP_202_ACCEPTED)
        self.assertRaises(KeywordsList.DoesNotExist,
                          saved_list.refresh_from_db)

    def test_get_keywords_list(self):
        my_list = KeywordsList.objects.create(
            user_email=self.user.email,
            name="My list",
            category="private"
        )
        another_list = KeywordsList.objects.create(
            user_email="donald.trump@mail.kz",
            name="Another list",
            category="private"
        )
        for i in range(1, 10):
            keyword = KeyWord.objects.create(text="kw #%s" % i)
            my_list.keywords.add(keyword)

        url = reverse(
            "keyword_tool_urls:kw_tool_saved_list_keywords",
            args=(another_list.id,)
        )
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_404_NOT_FOUND)

        url = reverse(
            "keyword_tool_urls:kw_tool_saved_list_keywords",
            args=(my_list.id,)
        )
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(
            response.data.keys(),
            {
                'items',
                'max_page',
                'current_page',
                'items_count',
            }
        )

        for item in response.data['items']:
            self.assertEqual(
                set(item.keys()),
                {
                    'search_volume',
                    'average_cpc',
                    'competition',
                    'keyword_text',
                    'monthly_searches',
                    'interests',
                    'updated_at',

                    'campaigns_count',
                    'average_cpm',
                    'average_cpv',
                    'clicks',
                    'cost',
                    'ctr',
                    'ctr_v',
                    'impressions',
                    'video_view_rate',
                    'video_views',
                }
            )

    def test_add_kws_to_keywords_list(self):
        my_list = KeywordsList.objects.create(
            user_email=self.user.email,
            name="My list",
            category="private"
        )
        ids = []
        for i in range(1, 10):
            keyword = KeyWord.objects.create(
                text="kw #%s" % i,
                search_volume=i,
            )
            ids.append(keyword.pk)

        base_url = reverse(
            "keyword_tool_urls:kw_tool_saved_list_keywords",
            args=(my_list.id,)
        )
        response = self.client.get(base_url)
        self.assertEqual(response.status_code, HTTP_200_OK)

        # update with 5 selected keywords
        selected_ids = ids[5:]
        with patch("keyword_tool.api.views.update_kw_list_stats") as patched_task:
            response = self.client.post(base_url, {'keywords': selected_ids})
            self.assertEqual(response.status_code, HTTP_202_ACCEPTED)
            self.assertEqual(patched_task.delay.call_count, 1)

        # check result
        response = self.client.get(base_url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data['items_count'], len(selected_ids))
        for i in response.data['items']:
            self.assertIn(i['keyword_text'], selected_ids)

    def test_add_existed_kw_to_keywords_list(self):
        my_list = KeywordsList.objects.create(
            user_email=self.user.email,
            name="My list",
            category="private"
        )
        ids = []
        for i in range(1, 10):
            keyword = KeyWord.objects.create(
                text="kw #%s" % i,
                search_volume=i,
            )
            if i % 2 == 0:
                my_list.keywords.add(keyword)
            ids.append(keyword.pk)

        base_url = reverse(
            "keyword_tool_urls:kw_tool_saved_list_keywords",
            args=(my_list.id,)
        )
        response = self.client.get(base_url)
        self.assertEqual(response.status_code, HTTP_200_OK)

        # update with 5 selected keywords
        with patch("keyword_tool.api.views.update_kw_list_stats") as patched_task:
            response = self.client.post(base_url, {'keywords': ids})
            self.assertEqual(response.status_code, HTTP_202_ACCEPTED)
            self.assertEqual(patched_task.delay.call_count, 1)

        # check result
        response = self.client.get(base_url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data['items_count'], len(ids))
        for i in response.data['items']:
            self.assertIn(i['keyword_text'], ids)

    def test_delete_kws_from_keywords_list(self):
        my_list = KeywordsList.objects.create(
            user_email=self.user.email,
            name="My list",
            category="private"
        )
        ids = []
        for i in range(1, 10):
            keyword = KeyWord.objects.create(
                text="kw #%s" % i,
                search_volume=i,
            )
            ids.append(keyword.pk)
            my_list.keywords.add(keyword)

        base_url = reverse(
            "keyword_tool_urls:kw_tool_saved_list_keywords",
            args=(my_list.id,)
        )
        response = self.client.get(base_url)
        self.assertEqual(response.status_code, HTTP_200_OK)

        # update with 5 selected keywords
        selected_ids = ids[5:]
        with patch("keyword_tool.api.views.update_kw_list_stats") as patched_task:
            response = self.client.delete(base_url, {'keywords': selected_ids})
            self.assertEqual(response.status_code, HTTP_202_ACCEPTED)
            self.assertEqual(patched_task.delay.call_count, 1)

        # check result
        response = self.client.get(base_url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data['items_count'],
                         len(ids) - len(selected_ids))
        for i in response.data['items']:
            self.assertNotIn(i['keyword_text'], selected_ids)
            self.assertIn(i['keyword_text'], ids)

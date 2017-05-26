from django.contrib.auth import get_user_model
from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK, HTTP_202_ACCEPTED, HTTP_404_NOT_FOUND

from keyword_tool.models import *
from saas.utils_tests import ExtendedAPITestCase as APITestCase


class KWToolSavedListTestCase(APITestCase):
    def setUp(self):
        self.user = self.create_test_user()

    def test_get_saved_lists(self):

        my_list = KeywordsList.objects.create(
            user_email=self.user.email,
            name="My list",
        )
        KeywordsList.objects.create(
            user_email="donald.trump@mail.kz",
            name="Another list",
        )

        self.url = reverse(
            "keyword_tool_urls:kw_tool_saved_lists"
        )
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 4)
        returned_list = response.data[0]
        self.assertEqual(returned_list['id'], my_list.id)
        self.assertEqual(returned_list['name'], my_list.name)

    def test_create_saved_lists(self):
        name = "My list"
        keywords = []
        for i in range(1, 10):
            keyword = KeyWord.objects.create(text="kw #%s" % i)
            keywords.append(keyword.text)

        self.url = reverse(
            "keyword_tool_urls:kw_tool_saved_lists"
        )

        response = self.client.post(
            self.url,
            dict(
                name=name,
                keywords=keywords,
            )
        )
        self.assertEqual(response.status_code, HTTP_202_ACCEPTED)
        self.assertEqual(response.data['name'], name)

    def test_update_saved_list(self):
        saved_list = KeywordsList.objects.create(
            user_email=self.user.email,
            name="My list",
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
        )
        another_list = KeywordsList.objects.create(
            user_email="donald.trump@mail.kz",
            name="Another list",
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

                    # 'campaigns_count',
                    # 'average_cpm',
                    # 'average_cpv',
                    # 'clicks',
                    # 'cost',
                    # 'ctr',
                    # 'ctr_v',
                    # 'impressions',
                    # 'video100rate',
                    # 'video25rate',
                    # 'video50rate',
                    # 'video75rate',
                    # 'video_view_rate',
                    # 'video_views',
                }
            )

    def test_add_kws_to_keywords_list(self):
        my_list = KeywordsList.objects.create(
            user_email=self.user.email,
            name="My list",
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
        response = self.client.post(base_url, {'keywords': selected_ids})
        self.assertEqual(response.status_code, HTTP_202_ACCEPTED)

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
        response = self.client.post(base_url, {'keywords': ids})
        self.assertEqual(response.status_code, HTTP_202_ACCEPTED)

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
        response = self.client.delete(base_url, {'keywords': selected_ids})
        self.assertEqual(response.status_code, HTTP_202_ACCEPTED)

        # check result
        response = self.client.get(base_url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data['items_count'],
                         len(ids) - len(selected_ids))
        for i in response.data['items']:
            self.assertNotIn(i['keyword_text'], selected_ids)
            self.assertIn(i['keyword_text'], ids)

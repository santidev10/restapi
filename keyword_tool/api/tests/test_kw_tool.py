from copy import deepcopy
from unittest import skip
from unittest.mock import patch
from urllib.parse import urlencode

from django.core.management import call_command
from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK
from rest_framework.test import APITransactionTestCase

from keyword_tool.models import *
from keyword_tool.settings import PREDEFINED_QUERIES
from utils.utils_tests import APITestUserMixin


class KWToolAPITestCase(APITransactionTestCase, APITestUserMixin):
    def setUp(self):
        self.user = self.create_test_user()

    def test_get_predefined_queries(self):
        self.url = reverse(
            "keyword_tool_urls:kw_tool_predefined_queries"
        )
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data, PREDEFINED_QUERIES)

    def test_interests_list(self):
        call_command("load_product_and_services")
        self.url = reverse("keyword_tool_urls:kw_tool_interests")
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(
            response.data,
            list(Interest.objects.values('id', 'name').order_by('name'))
        )

    @patch("keyword_tool.api.views.optimize_keyword")
    def test_optimize_query(self, optimize_keyword):
        call_command("load_product_and_services")
        optimize_keyword.return_value = deepcopy(RESP)
        query = "pokemon"
        url = reverse("keyword_tool_urls:kw_tool_optimize_query",
                      args=(query,))
        response = self.client.get(url)
        optimize_keyword.assert_called_with([query])
        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        self.assertEqual(
            set(data), {'items', 'current_page', 'max_page', 'items_count'}
        )
        self.assertEqual(data['current_page'], 1)
        self.assertEqual(data['max_page'], 1)
        self.assertEqual(data['items_count'], 2)

        # test again
        optimize_keyword.return_value = deepcopy(RESP)
        self.client.get(url)
        optimize_keyword.assert_called_once_with([query])  # was not called

    @skip("Unknown")
    @patch("keyword_tool.api.views.optimize_keyword")
    def test_optimize_query_similar_queries(self, optimize_keyword):
        call_command("load_product_and_services")
        optimize_keyword.return_value = deepcopy(RESP)
        query = "pokemon"
        url = reverse("keyword_tool_urls:kw_tool_optimize_query", args=(query,))
        response = self.client.get(url)
        optimize_keyword.assert_called(query)
        self.assertEqual(response.status_code, HTTP_200_OK)

        # test again
        optimize_keyword.return_value = [
            {
                'keyword_text': 'pokemon cards',
                'search_volume': 666,
                'interests': [10672, 13760, 10015, 13663, 55555]},

            {'keyword_text': 'pokemon x',
             'competition': 0.21171190748117472,
             'search_volume': 201000,
             'interests': [13376, 10015]}
        ]
        query = "batman"
        url = reverse("keyword_tool_urls:kw_tool_optimize_query",
                      args=(query,))
        response = self.client.get(url)
        optimize_keyword.assert_called_with(query)
        self.assertEqual(response.status_code, HTTP_200_OK)

    @patch("keyword_tool.api.views.optimize_keyword")
    def test_optimize_query_sort_volume(self, optimize_keyword):
        optimize_keyword.return_value = deepcopy(RESP)
        base_url = reverse("keyword_tool_urls:kw_tool_optimize_query",
                           args=("pokemon",))

        url = "{}?sort_by={}".format(base_url, 'search_volume')
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data['items']), 2)
        first, second = response.data['items']
        self.assertGreater(first['search_volume'], second['search_volume'])

    @patch("keyword_tool.api.views.optimize_keyword")
    def test_optimize_query_filter(self, optimize_keyword):
        call_command("load_product_and_services")
        optimize_keyword.return_value = deepcopy(RESP)
        base_url = reverse("keyword_tool_urls:kw_tool_optimize_query",
                           args=("pokemon",))

        filters = [
            dict(min_volume=450000, max_volume=500000),
            dict(min_competition=.1, max_competition=.5),
            dict(interests=",".join(['13760', '13417'])),
            dict(included=",".join(['pokemon', 'cards'])),
            dict(excluded=",".join(['porno', 'cards'])),
            dict(search="Pokemon X"),
        ]

        for f in filters:
            url = base_url + '?' + urlencode(f)
            response = self.client.get(url)
            self.assertEqual(response.status_code, HTTP_200_OK)
            self.assertEqual(len(response.data['items']), 1)


RESP = [{'monthly_searches': [{'label': '2015-10', 'value': 246000},
                              {'label': '2015-11', 'value': 301000},
                              {'label': '2015-12', 'value': 368000},
                              {'label': '2016-1', 'value': 301000},
                              {'label': '2016-2', 'value': 301000},
                              {'label': '2016-3', 'value': 368000},
                              {'label': '2016-4', 'value': 301000},
                              {'label': '2016-5', 'value': 368000},
                              {'label': '2016-6', 'value': 301000},
                              {'label': '2016-7', 'value': 550000},
                              {'label': '2016-8', 'value': 673000},
                              {'label': '2016-9', 'value': 823000}],
         'keyword_text': 'pokemon cards',
         'competition': 0.9386275922025674,
         'search_volume': 450000,
         'interests': [10672, 13760, 12998, 11943, 10135, 10729, 13417,
                       10125, 10013, 13518, 10015, 13663],
         'average_cpc': 0.161893},

        {'monthly_searches': [{'label': '2015-10', 'value': 110000},
                              {'label': '2015-11', 'value': 110000},
                              {'label': '2015-12', 'value': 135000},
                              {'label': '2016-1', 'value': 135000},
                              {'label': '2016-2', 'value': 165000},
                              {'label': '2016-3', 'value': 201000},
                              {'label': '2016-4', 'value': 135000},
                              {'label': '2016-5', 'value': 135000},
                              {'label': '2016-6', 'value': 135000},
                              {'label': '2016-7', 'value': 368000},
                              {'label': '2016-8', 'value': 368000},
                              {'label': '2016-9', 'value': 201000}],
         'keyword_text': 'pokemon x',
         'competition': 0.21171190748117472,
         'search_volume': 201000,
         'interests': [13376, 10019, 12998, 13383, 11943, 11945, 10729,
                       10125, 11950, 10672, 10135, 10167, 10013, 10015],
         'average_cpc': 0.171361}]

from unittest.mock import patch

from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK
from rest_framework.reverse import reverse

from es_components.models.keyword import Keyword

from utils.utittests.test_case import ExtendedAPITestCase
from utils.utittests.es_components_patcher import SearchDSLPatcher


class KeywordRetrieveUpdateApiViewTestCase(ExtendedAPITestCase):
    def test_get_keyword(self):
        keyword = "#tigerzindahai"
        self.create_test_user()
        url = reverse("keyword_api_urls:keywords", args=(keyword,))
        with patch("es_components.managers.keyword.KeywordManager.model.get", return_value=Keyword(id="keyword")):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)

    def test_get_keywords(self):
        url = reverse("keyword_api_urls:keyword_list")

        self.create_admin_user()
        with patch("es_components.managers.keyword.KeywordManager.search", return_value=SearchDSLPatcher()):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)


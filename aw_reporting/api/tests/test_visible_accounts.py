from django.core.urlresolvers import reverse
from django.http import QueryDict
from rest_framework.status import HTTP_200_OK

from aw_reporting.api.urls.names import Name
from saas.urls.namespaces import Namespace
from utils.utils_tests import ExtendedAPITestCase


class VisibleAccountsTestCase(ExtendedAPITestCase):
    url = reverse(Namespace.AW_REPORTING + ":" + Name.Admin.VISIBLE_ACCOUNTS)

    def test_success(self):
        user = self.create_admin_user()
        query_params = QueryDict(mutable=True)
        query_params.update(user_id=user.id)
        url = "?".join([self.url, query_params.urlencode()])

        response = self.client.get(url)

        self.assertEqual(response.status_code, HTTP_200_OK)

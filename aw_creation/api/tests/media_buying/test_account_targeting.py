from django.http import QueryDict

from aw_creation.api.urls.names import Name
from aw_creation.api.urls.namespace import Namespace
from aw_creation.models import AccountCreation
from aw_reporting.models import Account
from saas.urls.namespaces import Namespace as RootNamespace
from utils.unittests.reverse import reverse
from utils.unittests.test_case import ExtendedAPITestCase


class MediaBuyingAccountTargetingTestCase(ExtendedAPITestCase):
    def _get_url(self, account_creation_id):
        return reverse(
            Name.MediaBuying.ACCOUNT_TARGETING,
            [RootNamespace.AW_CREATION, Namespace.MEDIA_BUYING],
            args=(account_creation_id,),
        )

    def test_get_success(self):
        user = self.create_admin_user()
        account = Account.objects.create(id=1, name="",
                                         skip_creating_account_creation=True)
        account_creation = AccountCreation.objects.create(
            name="", is_managed=False, owner=user,
            account=account, is_approved=True)
        query_prams = QueryDict("targeting=all").urlencode()
        url = f"{self._get_url(account_creation.id)}?{query_prams}"
        response = self.client.get(url)
        data = response.data
        self.assertEqual(set(data.keys()), {"summary", "current_page", "items", "items_count", "max_page"})

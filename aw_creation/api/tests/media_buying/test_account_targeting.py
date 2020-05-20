import json
from datetime import date
from datetime import timedelta
from itertools import product

import pytz
from django.utils import timezone
from django.http import QueryDict
from rest_framework.status import HTTP_200_OK

from aw_creation.api.urls.names import Name
from aw_creation.api.urls.namespace import Namespace
from aw_creation.models import AccountCreation
from aw_reporting.demo.data import DEMO_ACCOUNT_ID
from aw_reporting.demo.recreate_demo_data import recreate_demo_data
from aw_reporting.models import Account
from aw_reporting.models import AdGroup
from aw_reporting.models import AdGroupStatistic
from aw_reporting.models import Campaign
from aw_reporting.models import CampaignStatistic
from aw_reporting.models import CityStatistic
from aw_reporting.models import Flight
from aw_reporting.models import GeoTarget
from aw_reporting.models import OpPlacement
from aw_reporting.models import Opportunity
from aw_reporting.models import SalesForceGoalType
from es_components.tests.utils import ESTestCase
from saas.urls.namespaces import Namespace as RootNamespace
from userprofile.constants import UserSettingsKey
from utils.unittests.int_iterator import int_iterator
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

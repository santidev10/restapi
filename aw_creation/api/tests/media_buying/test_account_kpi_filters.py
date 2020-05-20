import json
from datetime import date
from datetime import timedelta
from itertools import product

import pytz
from django.utils import timezone
from django.http import QueryDict
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_403_FORBIDDEN
from rest_framework.status import HTTP_404_NOT_FOUND

from aw_creation.api.urls.names import Name
from aw_creation.api.urls.namespace import Namespace
from aw_creation.api.views.media_buying.constants import REPORT_CONFIG
from aw_creation.models import AccountCreation
from aw_reporting.demo.data import DEMO_ACCOUNT_ID
from aw_reporting.demo.recreate_demo_data import recreate_demo_data
from aw_reporting.models import Account
from aw_reporting.models import AdGroup
from aw_reporting.models import AdGroupStatistic
from aw_reporting.models import Campaign
from aw_reporting.models import GenderStatistic
from aw_reporting.models import ParentStatistic
from aw_reporting.models import AgeRangeStatistic
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


class MediaBuyingAccountKpiFiltersTestCase(ExtendedAPITestCase):
    def _get_url(self, account_creation_id):
        return reverse(
            Name.MediaBuying.ACCOUNT_KPI_FILTERS,
            [RootNamespace.AW_CREATION, Namespace.MEDIA_BUYING],
            args=(account_creation_id,),
        )

    def test_no_permission_fail(self):
        self.create_test_user()
        account = Account.objects.create(id=1, name="")
        query_prams = QueryDict("targeting=all").urlencode()
        url = f"{self._get_url(account.account_creation.id)}?{query_prams}"
        user_settings = {
            UserSettingsKey.VISIBLE_ACCOUNTS: [account.id],
        }
        with self.patch_user_settings(**user_settings):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)

    def test_not_visible_account(self):
        user = self.create_admin_user()
        account = Account.objects.create(id=1, name="")
        query_prams = QueryDict("targeting=all").urlencode()
        url = f"{self._get_url(account.account_creation.id)}?{query_prams}"
        user_settings = {
            UserSettingsKey.VISIBLE_ACCOUNTS: [],
        }
        with self.patch_user_settings(**user_settings):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_404_NOT_FOUND)

    def test_get_success(self):
        user = self.create_admin_user()
        account = Account.objects.create(id=1, name="")
        query_prams = QueryDict("targeting=all").urlencode()
        url = f"{self._get_url(account.account_creation.id)}?{query_prams}"
        user_settings = {
            UserSettingsKey.VISIBLE_ACCOUNTS: [account.id],
        }
        with self.patch_user_settings(**user_settings):
            response = self.client.get(url)
        data = response.data
        self.assertEqual(set(data.keys()), set(REPORT_CONFIG["all"]["aggregations"]))

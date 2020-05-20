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
from aw_reporting.models.salesforce_constants import SalesForceGoalType
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


class MediaBuyingAccountBreakoutTestCase(ExtendedAPITestCase):
    def _get_url(self, account_creation_id):
        return reverse(
            Name.MediaBuying.ACCOUNT_BREAKOUT,
            [RootNamespace.AW_CREATION, Namespace.MEDIA_BUYING],
            args=(account_creation_id,),
        )

    def test_no_permission_fail(self):
        self.create_test_user()
        account = Account.objects.create(id=1, name="")
        url = f"{self._get_url(account.account_creation.id)}"
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
        op = Opportunity.objects.create()
        pl_1 = OpPlacement.objects.create(id=f"id_{next(int_iterator)}", name=f"pl_{next(int_iterator)}", opportunity=op, goal_type_id=SalesForceGoalType.CPM)
        pl_2 = OpPlacement.objects.create(id=f"id_{next(int_iterator)}", name=f"pl_{next(int_iterator)}", opportunity=op, goal_type_id=SalesForceGoalType.CPV)
        campaign_1 = Campaign.objects.create(name=f"c_{next(int_iterator)}", account=account, salesforce_placement=pl_1)
        campaign_2 = Campaign.objects.create(name=f"c_{next(int_iterator)}", account=account, salesforce_placement=pl_2)
        ad_group_1 = AdGroup.objects.create(name=f"a_{next(int_iterator)}", campaign=campaign_1, cpm_bid=2.5)
        ad_group_2 = AdGroup.objects.create(name=f"a_{next(int_iterator)}", campaign=campaign_2, cpv_bid=1.54)
        query_prams = QueryDict(f"ad_group_ids={ad_group_1.id},{ad_group_2.id}").urlencode()
        url = f"{self._get_url(account.account_creation.id)}?{query_prams}"
        user_settings = {
            UserSettingsKey.VISIBLE_ACCOUNTS: [account.id],
        }
        with self.patch_user_settings(**user_settings):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        print(response.data)

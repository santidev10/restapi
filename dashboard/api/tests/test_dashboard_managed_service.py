from datetime import date
from datetime import datetime
from datetime import timedelta

from django.conf import settings
from django.test import override_settings
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_403_FORBIDDEN

from aw_reporting.api.tests.base import AwReportingAPITestCase
from aw_reporting.models import AWAccountPermission
from aw_reporting.models import AWConnection
from aw_reporting.models import AWConnectionToUserRelation
from aw_reporting.models import Account
from aw_reporting.models import AdGroup
from aw_reporting.models import Campaign
from aw_reporting.models import CampaignStatistic
from aw_reporting.models import VideoCreative
from aw_reporting.models import VideoCreativeStatistic
from dashboard.api.urls.names import DashboardPathName
from saas.urls.namespaces import Namespace as RootNamespace
from userprofile.constants import StaticPermissions
from userprofile.constants import UserSettingsKey
from utils.demo.recreate_test_demo_data import recreate_test_demo_data
from utils.unittests.reverse import reverse


class DashboardManagedServiceListAPITestCase(AwReportingAPITestCase):

    url = reverse(DashboardPathName.DASHBOARD_MANAGED_SERVICE, [RootNamespace.DASHBOARD])

    details_keys = [
        "ctr",
        "ctr_v",
        "id",
        "aw_cid",
        "name",
        "thumbnail",
        "viewability",
        "video_view_rate",
        "completion_rate"
    ]

    def setUp(self, user=None):
        self.user = self.create_test_user() if not user else user
        self.user.add_custom_user_permission("view_dashboard")
        self.mcc_account = Account.objects.create(can_manage_clients=True)
        aw_connection = AWConnection.objects.create(refresh_token="token")
        AWAccountPermission.objects.create(aw_connection=aw_connection, account=self.mcc_account)
        AWConnectionToUserRelation.objects.create(connection=aw_connection, user=self.user)

    def __set_non_admin_user_with_account(self, account_id):
        user = self.user
        user.is_staff = False
        user.is_superuser = False
        user.update_access([{"name": "Tools", "value": True}])
        user.aw_settings[UserSettingsKey.VISIBLE_ACCOUNTS] = [account_id]
        user.save()

    def test_success_get(self):
        account = Account.objects.create(name="")
        account.managers.add(self.mcc_account)
        campaign = Campaign.objects.create(name="", account=account)
        ad_group = AdGroup.objects.create(name="", campaign=campaign)
        creative1 = VideoCreative.objects.create(id="SkubJruRo8w")
        creative2 = VideoCreative.objects.create(id="siFHgF9TOVA")
        action_date = datetime.now()
        VideoCreativeStatistic.objects.create(creative=creative1, date=action_date,
                                              ad_group=ad_group,
                                              impressions=10)
        VideoCreativeStatistic.objects.create(creative=creative2, date=action_date,
                                              ad_group=ad_group,
                                              impressions=12)
        self.user.perms[StaticPermissions.MANAGED_SERVICE__VISIBLE_ALL_ACCOUNTS] = True
        self.user.save()
        with override_settings(MCC_ACCOUNT_IDS=[self.mcc_account.id]):
            response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(
            set(response.data.keys()),
            {
                "max_page",
                "items_count",
                "items",
                "current_page",
            }
        )
        self.assertEqual(response.data["items_count"], 1)
        self.assertEqual(len(response.data["items"]), 1)
        item = response.data["items"][0]
        self.assertEqual(set(item.keys()), set(self.details_keys))

    def test_get_chf_account_creation_list_queryset(self):
        """
        make sure only selected accounts are present in response
        """
        recreate_test_demo_data()
        chf_account = Account.objects.create(
            id=settings.CHANNEL_FACTORY_ACCOUNT_ID, name="")
        expected_account_id = 1
        managed_account = Account.objects.create(
            id=expected_account_id, name="")
        managed_account.managers.add(chf_account)
        Account.objects.create(name="")
        Account.objects.create(name="")
        Account.objects.create(name="")
        self.__set_non_admin_user_with_account(managed_account.id)

        self.user.perms[StaticPermissions.MANAGED_SERVICE__VISIBLE_ALL_ACCOUNTS] = True
        self.user.save()
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        aw_cids = {item["aw_cid"] for item in response.data["items"]}
        self.assertEqual(aw_cids, {expected_account_id})

    def test_statistics(self):
        """
        ensure stats are correct on list response
        """
        chf_mcc_account = Account.objects.create(id=settings.CHANNEL_FACTORY_ACCOUNT_ID, can_manage_clients=True)
        account = Account.objects.create()
        account.managers.add(chf_mcc_account)
        account.save()

        days_count = 5
        clicks = 32
        impressions = 1234
        video_views = 345
        video_views_100_quartile = 31
        viewability = 23

        campaign = Campaign.objects.create(
            account=account,
            impressions=impressions * days_count,
            active_view_viewability=viewability,
            video_views=video_views * days_count,
            clicks=clicks * days_count,
        )

        dates = [date(2019, 1, 1) + timedelta(days=i) for i in range(days_count)]
        for dt in dates:
            CampaignStatistic.objects.create(
                campaign=campaign,
                date=dt,
                video_views_100_quartile=video_views_100_quartile,
                video_views=video_views,
                impressions=impressions,
                clicks=clicks,
            )
        self.user.perms[StaticPermissions.MANAGED_SERVICE__VISIBLE_ALL_ACCOUNTS] = True
        self.user.save()
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response.data["items_count"], 1)
        data = response.data
        item = data['items'][0]
        self.assertEqual(item["viewability"], viewability)
        self.assertEqual(item["completion_rate"], (video_views_100_quartile / impressions) * 100)
        self.assertEqual(item['video_view_rate'], (video_views / impressions) * 100)
        self.assertEqual(item['ctr'], (clicks / impressions) * 100)
        self.assertEqual(item['ctr_v'], (clicks / video_views) * 100)

    def test_extra_data_response_forbidden_to_non_admin(self):
        """
        ensure non-staff users are forbidden from getting extra data
        """
        response = self.client.get(self.url + f"?account_id=123")
        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)

    def test_extra_data_response_fields(self):
        """
        test for presence of extra data fields
        """
        chf_mcc_account = Account.objects.create(id=settings.CHANNEL_FACTORY_ACCOUNT_ID, can_manage_clients=True)
        account = Account.objects.create()
        account.managers.add(chf_mcc_account)
        account.save()
        self.user.perms[StaticPermissions.MANAGED_SERVICE__VISIBLE_ALL_ACCOUNTS] = True
        self.user.save()
        response = self.client.get(self.url + f"?account_id={account.id}")
        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        self.assertEqual(set(data.keys()), set(['pacing', 'margin', 'cpv']))
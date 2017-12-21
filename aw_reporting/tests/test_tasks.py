from datetime import datetime

import pytz
from django.test import testcases

import aw_reporting.tasks as aw_tasks
from aw_reporting.adwords_api import get_web_app_client
from aw_reporting.models import AWConnection, AWConnectionToUserRelation, AWAccountPermission, Account, Ad, \
    AdGroupStatistic, AdGroup, Campaign
from userprofile.models import UserProfile


class GetAdsTestCase(testcases.TestCase):
    @classmethod
    def setUpTestData(cls):
        user = UserProfile.objects.create(email='test@test.com')
        account = Account.objects.create(id='7155851537', name='Hall9000', can_manage_clients=True)
        cls.connection = AWConnection.objects.create(email="anna.chumak1409@gmail.com",
                                                     refresh_token='1/MJsHAtsAl1YYus3lMX0Tr_oCFGzHbZn7oupW-2SyAcs')
        AWAccountPermission.objects.create(aw_connection=cls.connection, account=account, can_read=True)

        AWConnectionToUserRelation.objects.create(
            connection=cls.connection,
            user=user
        )
        cls.aw_client = get_web_app_client(
            refresh_token=cls.connection.refresh_token,
        )
        cls.aw_client.SetClientCustomerId(account.id)

    def setUp(self):
        self.test_account = Account.objects.create(
            id='6832554362',
            name='Pacing automation script verification #c7cd556404f7',
            timezone="America/Los_Angeles",
            can_manage_clients=False,
            is_test_account=False,
            visible=True,
            update_time=None
        )
        self.today = datetime.now(tz=pytz.utc).date()
        self.aw_client.SetClientCustomerId(self.test_account.id)
        campaign = Campaign.objects.create(id='1', name='', account=self.test_account)
        ad_group = AdGroup.objects.create(campaign=campaign)
        AdGroupStatistic.objects.create(ad_group=ad_group, date=datetime(2017, 1, 1), average_position=1)
        AdGroupStatistic.objects.create(ad_group=ad_group, date=datetime(2018, 1, 1), average_position=1)

    def test_is_disapproved_stored(self):
        aw_tasks.get_ads(self.aw_client, self.test_account, self.today)
        disapproved_ad = Ad.objects.filter(is_disapproved=True).first()
        self.assertTrue(disapproved_ad is not None)

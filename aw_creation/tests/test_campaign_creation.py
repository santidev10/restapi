from datetime import datetime, timedelta
from saas.utils_tests import ExtendedAPITestCase
from aw_creation.models import AccountCreation, CampaignCreation, AdGroupCreation, AdCreation
from aw_reporting.models import DEFAULT_TIMEZONE
from time import sleep
import pytz


class CampaignCreationTestCase(ExtendedAPITestCase):

    def test_creation_dates_no_dates(self):
        user = self.create_test_user()
        campaign = CampaignCreation.objects.create(
            name="",  account_creation=AccountCreation.objects.create(name="", owner=user),
        )
        creation_start, start, end = campaign.get_creation_dates()
        self.assertEqual(creation_start, datetime.now(tz=pytz.timezone(DEFAULT_TIMEZONE)).date())
        self.assertIsNone(start)
        self.assertIsNone(end)

    def test_creation_dates_start_in_past(self):
        user = self.create_test_user()
        today = datetime.now(tz=pytz.timezone(DEFAULT_TIMEZONE)).date()
        campaign = CampaignCreation.objects.create(
            name="",  account_creation=AccountCreation.objects.create(name="", owner=user),
            start=today - timedelta(days=1)
        )
        creation_start, start, end = campaign.get_creation_dates()
        self.assertEqual(creation_start, today)
        self.assertEqual(start, today)
        self.assertIsNone(end)

    def test_creation_dates_both_in_past(self):
        user = self.create_test_user()
        today = datetime.now(tz=pytz.timezone(DEFAULT_TIMEZONE)).date()
        start = today - timedelta(days=2)
        end = today - timedelta(days=1)
        campaign = CampaignCreation.objects.create(
            name="",  account_creation=AccountCreation.objects.create(name="", owner=user),
            start=start, end=end,
        )
        creation_start, start, end = campaign.get_creation_dates()
        self.assertEqual(creation_start, start)
        self.assertEqual(start, start)
        self.assertEqual(end, end)

    def test_update_times(self):
        user = self.create_test_user()
        account_creation = AccountCreation.objects.create(name="", owner=user)
        campaign_creation = CampaignCreation.objects.create(name="", account_creation=account_creation)
        ad_group_creation = AdGroupCreation.objects.create(name="", campaign_creation=campaign_creation)
        ad_creation = AdCreation.objects.create(name="", ad_group_creation=ad_group_creation)

        sleep(2)

        ad_creation.save()  # this updates all the parents

        time = ad_creation.updated_at.replace(microsecond=0)

        ad_group_creation.refresh_from_db()
        self.assertLessEqual((ad_group_creation.updated_at.replace(microsecond=0) - time).seconds, 1)

        campaign_creation.refresh_from_db()
        self.assertLessEqual((campaign_creation.updated_at.replace(microsecond=0) - time).seconds, 1)

        account_creation.refresh_from_db()
        self.assertLessEqual((account_creation.updated_at.replace(microsecond=0) - time).seconds, 1)

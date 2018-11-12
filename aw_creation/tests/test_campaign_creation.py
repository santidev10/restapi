from datetime import timedelta
from time import sleep

from aw_creation.models import AccountCreation, CampaignCreation, \
    AdGroupCreation, AdCreation
from utils.datetime import now_in_default_tz
from utils.utittests.test_case import ExtendedAPITestCase


class CampaignCreationTestCase(ExtendedAPITestCase):

    def test_creation_dates_no_dates(self):
        user = self.create_test_user()
        campaign = CampaignCreation.objects.create(
            name="",  account_creation=AccountCreation.objects.create(name="", owner=user),
        )
        creation_start, start, end = campaign.get_creation_dates()
        self.assertEqual(creation_start, now_in_default_tz().date())
        self.assertIsNone(start)
        self.assertIsNone(end)

    def test_creation_dates_start_in_past(self):
        user = self.create_test_user()
        today = now_in_default_tz().date()
        campaign = CampaignCreation.objects.create(
            name="",  account_creation=AccountCreation.objects.create(name="", owner=user),
            start=today - timedelta(days=1),
            end=today + timedelta(days=1),
        )
        creation_start, start, end = campaign.get_creation_dates()
        self.assertEqual(creation_start, today)
        self.assertIsNone(start)  # so it is not trying to update campaign's start
        self.assertEqual(end, campaign.end)

    def test_creation_dates_both_in_past(self):
        user = self.create_test_user()
        today = now_in_default_tz().date()
        start = today - timedelta(days=2)
        end = today - timedelta(days=1)
        campaign = CampaignCreation.objects.create(
            name="",  account_creation=AccountCreation.objects.create(name="", owner=user),
            start=start, end=end,
        )
        creation_start, start, end = campaign.get_creation_dates()
        self.assertEqual(creation_start, campaign.start)
        self.assertIsNone(start, start)
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

from datetime import datetime, timedelta
from saas.utils_tests import ExtendedAPITestCase
from aw_creation.models import AccountCreation, CampaignCreation
from aw_reporting.models import DEFAULT_TIMEZONE
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

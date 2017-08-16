from django.core.urlresolvers import reverse
from django.utils import timezone
from aw_creation.models import *
from aw_reporting.models import *
from datetime import timedelta
from aw_reporting.api.tests.base import AwReportingAPITestCase


class ChangedAccountsAPITestCase(AwReportingAPITestCase):

    def test_success_patch(self):
        user = self.create_test_user(auth=False)
        account = Account.objects.create(id="123", name="")
        account_creation = AccountCreation.objects.create(
            name="", owner=user, account=account, is_approved=True
        )
        campaign_creation = CampaignCreation.objects.create(name="", account_creation=account_creation, budget="20.2")
        ad_group_creation = AdGroupCreation.objects.create(name="", campaign_creation=campaign_creation, max_rate="1.1")
        ad_creation = AdCreation.objects.create(
            name="", ad_group_creation=ad_group_creation,
            video_url="https://www.youtube.com/watch?v=1UtCe4dO2Tk",
            final_url="https://www.youtube.com/watch?v=1UtCe4dO2Tk",
            display_url="https://www.youtube.com/watch?v=1UtCe4dO2Tk",
        )
        empty_ad_creation = AdCreation.objects.create(name="", ad_group_creation=ad_group_creation)
        # --
        url = reverse("aw_creation_urls:aw_creation_change_status", args=(account.id,))
        account_creation.refresh_from_db()
        self.client.patch(
            url, json.dumps({"updated_at": str(account_creation.updated_at - timedelta(seconds=2))}),
            content_type='application/json',
        )
        account_creation.refresh_from_db()
        self.assertEqual(account_creation.is_changed, True)

        self.client.patch(
            url, json.dumps({"updated_at": str(account_creation.updated_at)}), content_type='application/json',
        )
        account_creation.refresh_from_db()
        self.assertIs(account_creation.is_changed, False)
        campaign_creation.refresh_from_db()
        self.assertIs(campaign_creation.is_changed, False)
        ad_group_creation.refresh_from_db()
        self.assertIs(ad_group_creation.is_changed, False)
        ad_creation.refresh_from_db()
        self.assertIs(ad_creation.is_changed, False)
        empty_ad_creation.refresh_from_db()
        self.assertIs(empty_ad_creation.is_changed, True)



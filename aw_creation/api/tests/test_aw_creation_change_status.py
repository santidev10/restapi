import json

from django.core.urlresolvers import reverse

from aw_creation.models import AccountCreation, CampaignCreation, \
    AdGroupCreation, AdCreation
from aw_reporting.api.tests.base import AwReportingAPITestCase, Account


class ChangedAccountsAPITestCase(AwReportingAPITestCase):

    def test_success_patch(self):
        user = self.create_test_user(auth=False)
        account = Account.objects.create(id="123", name="", skip_creating_account_creation=True)
        account_creation = AccountCreation.objects.create(
            name="", owner=user, account=account, is_approved=True
        )
        campaign_creation = CampaignCreation.objects.create(
            name="", account_creation=account_creation, budget="20.2")
        ad_group_creation = AdGroupCreation.objects.create(
            name="", campaign_creation=campaign_creation, max_rate="1.1")
        AdCreation.objects.create(
            name="", ad_group_creation=ad_group_creation,
            video_url="https://www.youtube.com/watch?v=1UtCe4dO2Tk",
            final_url="https://www.youtube.com/watch?v=1UtCe4dO2Tk",
            display_url="https://www.youtube.com/watch?v=1UtCe4dO2Tk",
        )
        AdCreation.objects.create(name="", ad_group_creation=ad_group_creation)
        url = reverse("aw_creation_urls:aw_creation_change_status", args=(account.id,))
        campaigns = [
                dict(id="123", name=campaign_creation.unique_name, ad_groups=[
                    dict(id="456", name=ad_group_creation.unique_name)])
        ]
        self.client.patch(url, json.dumps({"campaigns": campaigns}), content_type='application/json')
        campaign_creation.refresh_from_db()
        ad_group_creation.refresh_from_db()
        self.assertEqual(campaign_creation.campaign_id, campaigns[0]['id'])
        self.assertEqual(ad_group_creation.ad_group_id, campaigns[0]['ad_groups'][0]['id'])

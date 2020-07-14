from django.test import TransactionTestCase
from django.test import override_settings

from aw_reporting.demo.data import DEMO_ACCOUNT_ID
from aw_reporting.demo.data import DEMO_BRAND
from aw_reporting.demo.data import DEMO_SF_ACCOUNT
from aw_reporting.demo.recreate_demo_data import recreate_demo_data
from aw_reporting.models import Account
from aw_reporting.models import AdGroup
from aw_reporting.models import Campaign
from aw_reporting.models import OpPlacement
from aw_reporting.models import Opportunity
from utils.unittests.str_iterator import str_iterator


class RecreateDemoDataTestCase(TransactionTestCase):
    def _create_source_root(self, opp_data=None, campaign_data=None):
        opportunity = Opportunity.objects.create(id=next(str_iterator), **(opp_data or dict()))
        placement = OpPlacement.objects.create(id=next(str_iterator), opportunity=opportunity)
        account = Account.objects.create()
        Campaign.objects.create(salesforce_placement=placement, account=account, **(campaign_data or dict()))

        return opportunity, account

    def test_brand_replace(self):
        _, account = self._create_source_root(dict(brand="some brand"))

        with override_settings(DEMO_SOURCE_ACCOUNT_ID=account.id):
            recreate_demo_data()

        opportunity = Opportunity.objects.get(id=DEMO_ACCOUNT_ID)
        self.assertEqual(opportunity.brand, DEMO_BRAND)

    def test_campaign_name(self):
        _, account = self._create_source_root(campaign_data=dict(name="Source Campaign 123"))

        with override_settings(DEMO_SOURCE_ACCOUNT_ID=account.id):
            recreate_demo_data()

        demo_campaign = Campaign.objects.filter(account_id=DEMO_ACCOUNT_ID).first()

        self.assertEqual(demo_campaign.name, "Campaign #demo1")

    def test_agency(self):
        _, account = self._create_source_root()

        with override_settings(DEMO_SOURCE_ACCOUNT_ID=account.id):
            recreate_demo_data()

        opportunity = Opportunity.objects.get(id=DEMO_ACCOUNT_ID)
        self.assertEqual(opportunity.account.name, DEMO_SF_ACCOUNT)

    def test_ad_group_name(self):
        _, account = self._create_source_root()
        campaign = account.campaigns.first()
        ad_group = AdGroup.objects.create(campaign=campaign, name="Source AdGroup")

        with override_settings(DEMO_SOURCE_ACCOUNT_ID=account.id):
            recreate_demo_data()

        demo_ad_group = AdGroup.objects.get(campaign__account_id=DEMO_ACCOUNT_ID)

        self.assertNotEqual(demo_ad_group.name, ad_group.name)
        self.assertEqual(demo_ad_group.name, "AdGroup #1:1")

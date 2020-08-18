import re
from traceback import format_exception

from django.test import TransactionTestCase
from django.test import override_settings

from aw_reporting.demo.data import DEMO_ACCOUNT_ID
from aw_reporting.demo.data import DEMO_BRAND
from aw_reporting.demo.recreate_demo_data import recreate_demo_data
from aw_reporting.models import Account
from aw_reporting.models import Ad
from aw_reporting.models import AdGroup
from aw_reporting.models import Campaign
from aw_reporting.models import OpPlacement
from aw_reporting.models import Opportunity
from aw_reporting.models import SFAccount
from utils.unittests.str_iterator import str_iterator


class RecreateDemoDataTestCase(TransactionTestCase):
    def _create_source_root(self, opp_data=None, campaign_data=None):
        campaign_data = campaign_data or dict()
        opportunity = Opportunity.objects.create(id=next(str_iterator), **(opp_data or dict()))
        pl_number = "PL000001"
        placement = OpPlacement.objects.create(id=next(str_iterator), opportunity=opportunity, number=pl_number,
                                               name=f"Placement {pl_number}")
        account = Account.objects.create()
        default_campaign_data = dict(salesforce_placement=placement, account=account, name=f"Campaign {pl_number}")
        Campaign.objects.create(**{**campaign_data, **default_campaign_data})
        print({**default_campaign_data, **campaign_data})

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
        source_sf_account_name = "Source SF Account"
        source_sf_account = SFAccount.objects.create(name=source_sf_account_name)
        source_opportunity, account = self._create_source_root(opp_data=dict(account_id=source_sf_account.id))

        with override_settings(DEMO_SOURCE_ACCOUNT_ID=account.id):
            recreate_demo_data()

        opportunity = Opportunity.objects.get(id=DEMO_ACCOUNT_ID)
        self.assertEqual(opportunity.account.name, source_sf_account_name)

    def test_ad_group_name(self):
        _, account = self._create_source_root()
        campaign = account.campaigns.first()
        ad_group = AdGroup.objects.create(campaign=campaign, name="Source AdGroup")

        with override_settings(DEMO_SOURCE_ACCOUNT_ID=account.id):
            recreate_demo_data()

        demo_ad_group = AdGroup.objects.get(campaign__account_id=DEMO_ACCOUNT_ID)

        self.assertNotEqual(demo_ad_group.name, ad_group.name)
        self.assertEqual(demo_ad_group.name, "AdGroup #1:1")

    def test_missing_ad_creation(self):
        _, account = self._create_source_root()
        campaign = account.campaigns.first()
        ad_group = AdGroup.objects.create(campaign=campaign)
        Ad.objects.create(ad_group=ad_group)

        with override_settings(DEMO_SOURCE_ACCOUNT_ID=account.id):
            try:
                recreate_demo_data()
            except Exception as ex:
                tb = "".join(format_exception(etype=type(ex), value=ex, tb=ex.__traceback__))
                self.fail(f"Failed with {type(ex)} \n {tb}")

    def test_generates_new_placement_number(self):
        _, account = self._create_source_root()

        with override_settings(DEMO_SOURCE_ACCOUNT_ID=account.id):
            recreate_demo_data()

        placements = OpPlacement.objects.all()
        placements_numbers = {placement.number for placement in placements}
        self.assertEqual(placements.count(), len(placements_numbers))
        for placement in placements:
            self.assertIn(placement.number, placement.name)
            self.assertIn(placement.number, placement.adwords_campaigns.first().name)

    def test_generates_new_opportunity_number(self):
        _, source_account = self._create_source_root()

        with override_settings(DEMO_SOURCE_ACCOUNT_ID=source_account.id):
            recreate_demo_data()

        opportunity = Opportunity.objects.get(id=DEMO_ACCOUNT_ID)
        account = Account.objects.get(id=DEMO_ACCOUNT_ID)
        self.assertEqual(1, Opportunity.objects.filter(number=opportunity.number).count())
        self.assertIn(opportunity.number, opportunity.name)
        self.assertIn(opportunity.number, account.name)

    def test_opportunity_name(self):
        _, source_account = self._create_source_root()

        with override_settings(DEMO_SOURCE_ACCOUNT_ID=source_account.id):
            recreate_demo_data()

        expected_name_pattern = r"Acme Instant Coffee Q2-Q3’20 OP\d+"
        opportunity = Opportunity.objects.get(id=DEMO_ACCOUNT_ID)
        account = Opportunity.objects.get(id=DEMO_ACCOUNT_ID)
        self.assertTrue(re.search(expected_name_pattern, opportunity.name))
        self.assertTrue(re.search(expected_name_pattern, account.name))

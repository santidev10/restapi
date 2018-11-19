from datetime import timedelta

from django.contrib.auth import get_user_model
from django.utils import timezone

from aw_creation.models import AccountCreation
from aw_creation.models import AdCreation
from aw_creation.models import AdGroupCreation
from aw_creation.models import CampaignCreation
from aw_creation.tasks import add_relation_between_report_and_creation_ad_groups
from aw_creation.tasks import add_relation_between_report_and_creation_ads
from aw_creation.tasks import add_relation_between_report_and_creation_campaigns
from aw_reporting.models import Account
from aw_reporting.models import Ad
from aw_reporting.models import AdGroup
from aw_reporting.models import Campaign
from utils.utittests.test_case import ExtendedAPITestCase


class AddRelationsTestCase(ExtendedAPITestCase):
    """
    Unfortunately there are two type of entities that the client treat as one thing
    Campaign and CampaignCreation
    AdGroup and AdGroupCreation
    We should manage them either independetly or together depends on context
    So we need some relations between them and we have two tasks to make the links
    """

    def test_link_campaigns(self):
        sync_at = timezone.now() + timedelta(seconds=60)
        owner = get_user_model().objects.create(email="1@dom.ua")
        account_creation = AccountCreation.objects.create(name="", owner=owner)
        campaign_creation_1 = CampaignCreation.objects.create(name="", account_creation=account_creation,
                                                              sync_at=sync_at)
        campaign_creation_2 = CampaignCreation.objects.create(name="", account_creation=account_creation,
                                                              sync_at=sync_at)
        account = Account.objects.create(id="1", name="",
                                         skip_creating_account_creation=True)
        account_creation.account = account
        account_creation.save()
        campaign_1 = Campaign.objects.create(
            id="1", account=account, name="ee, 1 #{}".format(campaign_creation_2.id),
        )
        Campaign.objects.create(id="2", name="# Campaign #", account=account)

        # run tasks
        add_relation_between_report_and_creation_campaigns()

        # check
        campaign_creation_1.refresh_from_db()
        campaign_creation_2.refresh_from_db()

        self.assertIsNone(campaign_creation_1.campaign_id)
        self.assertEqual(campaign_creation_2.campaign_id, campaign_1.id)

    def test_link_ad_groups(self):
        account = Account.objects.create(id="1", name="",
                                         skip_creating_account_creation=True)
        campaign = Campaign.objects.create(id="1", account=account, name="")

        sync_at = timezone.now() + timedelta(seconds=60)
        owner = get_user_model().objects.create(email="1@dom.ua")
        account_creation = AccountCreation.objects.create(name="", owner=owner, account=account)
        campaign_creation = CampaignCreation.objects.create(name="", account_creation=account_creation,
                                                            sync_at=sync_at, campaign=campaign)
        campaign_creation_2 = CampaignCreation.objects.create(name="", account_creation=account_creation,
                                                              sync_at=sync_at)
        ad_group_creation_1 = AdGroupCreation.objects.create(name="", campaign_creation=campaign_creation,
                                                             sync_at=sync_at)
        ad_group_creation_2 = AdGroupCreation.objects.create(name="", campaign_creation=campaign_creation,
                                                             sync_at=sync_at)
        ad_group_creation_3 = AdGroupCreation.objects.create(name="", campaign_creation=campaign_creation_2,
                                                             sync_at=sync_at)

        ad_group_1 = AdGroup.objects.create(id="1", campaign=campaign, name="-$#2 #{}".format(ad_group_creation_3.id))
        ad_group_2 = AdGroup.objects.create(id="2", campaign=campaign, name="AG #{}".format(ad_group_creation_2.id))

        # run tasks
        add_relation_between_report_and_creation_ad_groups()

        # test
        ad_group_creation_1.refresh_from_db()
        ad_group_creation_2.refresh_from_db()
        ad_group_creation_3.refresh_from_db()

        self.assertIsNone(ad_group_creation_1.ad_group_id)
        self.assertEqual(ad_group_creation_2.ad_group_id, ad_group_2.id)
        self.assertIsNone(ad_group_creation_3.ad_group_id,
                          "ad_group_1 contain its id, but campaign_1 isn't linked with campaign_creation_1")

    def test_link_ads(self):
        account = Account.objects.create(id="1", name="",
                                         skip_creating_account_creation=True)
        campaign = Campaign.objects.create(id="1", account=account, name="")
        ad_group_1 = AdGroup.objects.create(id="1", campaign=campaign, name="")
        ad_group_2 = AdGroup.objects.create(id="2", campaign=campaign, name="")

        sync_at = timezone.now() + timedelta(seconds=60)
        owner = get_user_model().objects.create(email="1@dom.ua")
        account_creation = AccountCreation.objects.create(name="", owner=owner, account=account)
        campaign_creation = CampaignCreation.objects.create(name="", account_creation=account_creation,
                                                            sync_at=sync_at, campaign=campaign)
        ad_group_creation = AdGroupCreation.objects.create(
            name="", campaign_creation=campaign_creation,
            sync_at=sync_at, ad_group=ad_group_1
        )
        ad_group_creation_2 = AdGroupCreation.objects.create(
            name="", campaign_creation=campaign_creation,
            sync_at=sync_at, ad_group=ad_group_2
        )
        ad_creation_1 = AdCreation.objects.create(name="", ad_group_creation=ad_group_creation, sync_at=sync_at)
        ad_creation_2 = AdCreation.objects.create(name="", ad_group_creation=ad_group_creation, sync_at=sync_at)
        ad_creation_3 = AdCreation.objects.create(name="", ad_group_creation=ad_group_creation_2, sync_at=sync_at)
        ad_creation_4 = AdCreation.objects.create(name="", ad_group_creation=ad_group_creation, sync_at=sync_at)

        ad_1 = Ad.objects.create(id="1", ad_group=ad_group_1, creative_name="-$#2 #{}".format(ad_creation_1.id))
        ad_2 = Ad.objects.create(id="2", ad_group=ad_group_1, creative_name="AG #{}".format(ad_creation_2.id))
        ad_3 = Ad.objects.create(id="3", ad_group=ad_group_1, creative_name="AG #{}".format(ad_creation_3.id))
        ad_4 = Ad.objects.create(id="4", ad_group=ad_group_2, creative_name="AG #{}".format(ad_creation_4.id))

        # run tasks
        add_relation_between_report_and_creation_ads()

        # test
        ad_creation_1.refresh_from_db()
        ad_creation_2.refresh_from_db()

        self.assertEqual(ad_creation_1.ad_id, ad_1.id)
        self.assertEqual(ad_creation_2.ad_id, ad_2.id)
        self.assertIsNone(ad_creation_3.ad_id, "AdGroup #1 != AdGroupCreation#2")
        self.assertIsNone(ad_creation_4.ad_id, "AdGroup #2 != AdGroupCreation#1")

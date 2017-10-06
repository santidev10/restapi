from saas.utils_tests import ExtendedAPITestCase
from django.contrib.auth import get_user_model
from django.utils import timezone
from datetime import timedelta
from aw_creation.tasks import add_relation_between_report_and_creation_ad_groups
from aw_creation.tasks import add_relation_between_report_and_creation_campaigns
from aw_creation.models import AccountCreation, CampaignCreation, AdGroupCreation
from aw_reporting.models import Account, Campaign, AdGroup


class AddRelationsTestCase(ExtendedAPITestCase):
    """
    Unfortunately there are two type of entities that the client treat as one thing
    Campaign and CampaignCreation
    AdGroup and AdGroupCreation
    We should manage them either independetly or together depends on context
    So we need some relations between them and we have two tasks to make the links
    """

    def test_link_items(self):
        sync_at = timezone.now() - timedelta(seconds=60)
        owner = get_user_model().objects.create(email="1@dom.ua")
        account_creation = AccountCreation.objects.create(name="", owner=owner)
        campaign_creation_1 = CampaignCreation.objects.create(
            name="", account_creation=account_creation, sync_at=sync_at)
        campaign_creation_2 = CampaignCreation.objects.create(
            name="", account_creation=account_creation, sync_at=sync_at)
        ad_group_creation_1 = AdGroupCreation.objects.create(
            name="", campaign_creation=campaign_creation_2, sync_at=sync_at)
        ad_group_creation_2 = AdGroupCreation.objects.create(
            name="", campaign_creation=campaign_creation_2, sync_at=sync_at)
        ad_group_creation_3 = AdGroupCreation.objects.create(
            name="", campaign_creation=campaign_creation_1, sync_at=sync_at)

        account = Account.objects.create(id="1", name="")
        account_creation.account = account
        account_creation.save()
        campaign_1 = Campaign.objects.create(
            id="1", account=account,  name="ee, 1 #{}".format(campaign_creation_2.id),
        )
        campaign_2 = Campaign.objects.create(id="2", name="# Campaign #", account=account)
        ad_group_1 = AdGroup.objects.create(id="1", campaign=campaign_1, name="-$#2 #{}".format(ad_group_creation_3.id))
        ad_group_2 = AdGroup.objects.create(id="2", campaign=campaign_1, name="AG #{}".format(ad_group_creation_2.id))

        # run tasks
        add_relation_between_report_and_creation_campaigns()
        add_relation_between_report_and_creation_ad_groups()
        # -- run tasks

        campaign_creation_1.refresh_from_db()
        campaign_creation_2.refresh_from_db()

        self.assertIsNone(campaign_creation_1.campaign_id)
        self.assertEqual(campaign_creation_2.campaign_id, campaign_1.id)

        ad_group_creation_1.refresh_from_db()
        ad_group_creation_2.refresh_from_db()
        ad_group_creation_3.refresh_from_db()

        self.assertIsNone(ad_group_creation_1.ad_group_id)
        self.assertEqual(ad_group_creation_2.ad_group_id, ad_group_2.id)
        self.assertIsNone(ad_group_creation_3.ad_group_id,
                          "ad_group_1 contain its id, but campaign_1 isn't linked with campaign_creation_1")



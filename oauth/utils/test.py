from oauth.constants import OAuthType
from oauth.models import Account
from oauth.models import AdGroup
from oauth.models import Campaign
from oauth.models import DV360Advertiser
from oauth.models import DV360Partner
from oauth.models import InsertionOrder
from utils.unittests.int_iterator import int_iterator


def create_dv360_resources(oauth_account):
    partner = DV360Partner.objects.create(id=next(int_iterator))
    advertiser = DV360Advertiser.objects.create(partner=partner, id=next(int_iterator))
    campaign = Campaign.objects.create(id=next(int_iterator), advertiser=advertiser, oauth_type=OAuthType.DV360.value)
    insertion_order = InsertionOrder.objects.create(id=next(int_iterator), campaign=campaign)
    oauth_account.dv360_advertisers.add(advertiser)
    return advertiser, campaign, insertion_order


def create_gads_resources(oauth_account):
    account = Account.objects.create(id=next(int_iterator))
    campaign = Campaign.objects.create(id=next(int_iterator), oauth_type=OAuthType.GOOGLE_ADS.value, account=account)
    ad_group = AdGroup.objects.create(id=next(int_iterator), oauth_type=OAuthType.GOOGLE_ADS.value, campaign=campaign)
    oauth_account.gads_accounts.add(account)
    return account, campaign, ad_group


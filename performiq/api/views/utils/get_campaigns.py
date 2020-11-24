from performiq.models import OAuthAccount
from performiq.models import Campaign
from performiq.models.constants import OAuthType
from performiq.api.serializers import CampaignSerializer
from utils.views import get_object


def get_campaigns(user):
    campaigns = {
        "google_ads": {},
        "dv360": {},
    }
    gads_account = get_object(OAuthAccount, user=user, oauth_type=OAuthType.GOOGLE_ADS.value, should_raise=False)
    dv360_account = get_object(OAuthAccount, user=user, oauth_type=OAuthType.DV360.value, should_raise=False)
    if gads_account:
        gads_qs = Campaign.objects.filter(account__oauth_account__user=user)
        gads_data = _get_serialized(gads_qs, gads_account)
        campaigns["google_ads"].update(gads_data)
    if dv360_account:
        dv360_qs = Campaign.objects.filter(advertiser__oauth_accounts__user=user)
        dv360_data = _get_serialized(dv360_qs, dv360_account)
        campaigns["dv360"].update(dv360_data)
    return campaigns


def _get_serialized(qs, account):
    campaigns = CampaignSerializer(qs, many=True).data
    data = {
        "email": account.email,
        "campaigns": campaigns,
    }
    return data

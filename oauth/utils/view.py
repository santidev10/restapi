from oauth.api.serializers import CampaignSerializer
from oauth.constants import OAuthType
from oauth.models import Campaign
from oauth.models import OAuthAccount
from utils.views import get_object


def get_campaigns(user):
    campaigns = {
        "google_ads": {},
        "dv360": {},
    }
    gads_oauth_account = get_object(OAuthAccount, user=user, oauth_type=OAuthType.GOOGLE_ADS.value, is_enabled=True,
                                    should_raise=False)
    dv360_account = get_object(OAuthAccount, user=user, oauth_type=OAuthType.DV360.value, is_enabled=True,
                               should_raise=False)
    if gads_oauth_account:
        gads_qs = Campaign.objects.filter(account__oauth_accounts__user=user)
        gads_data = _get_serialized(gads_qs, gads_oauth_account, CampaignSerializer)
        campaigns["google_ads"].update(gads_data)
    if dv360_account:
        dv360_qs = Campaign.objects.filter(advertiser__oauth_accounts__user=user)
        dv360_data = _get_serialized(dv360_qs, dv360_account, CampaignSerializer)
        campaigns["dv360"].update(dv360_data)
    return campaigns


def get_ad_groups(user):
    ad_groups = {
        "google_ads": {},
        "dv360": {},
    }
    gads_oauth_account = get_object(OAuthAccount, user=user, oauth_type=OAuthType.GOOGLE_ADS.value, is_enabled=True,
                                    should_raise=False)
    dv360_account = get_object(OAuthAccount, user=user, oauth_type=OAuthType.DV360.value, is_enabled=True,
                               should_raise=False)
    if gads_oauth_account:
        gads_qs = AdGroup.objects.filter(campaign__account__oauth_accounts__user=user)
        gads_data = _get_serialized(gads_qs, gads_oauth_account, AdGroupSerializer)
        ad_groups["google_ads"].update(gads_data)
    if dv360_account:
        dv360_qs = Campaign.objects.filter(campaign__advertiser__oauth_accounts__user=user)
        dv360_data = _get_serialized(dv360_qs, dv360_account, AdGroupSerializer)
        ad_groups["dv360"].update(dv360_data)
    return ad_groups


def _get_serialized(qs, account, serializer):
    serialized = serializer(qs, many=True).data
    data = {
        "email": account.email,
        "oauth_account_id": account.id,
        "oauth_account_synced": account.synced,
        "data": serialized,
    }
    return data


def _get_serialized(qs, account):
    campaigns = CampaignSerializer(qs, many=True).data
    data = {
        "email": account.email,
        "oauth_account_id": account.id,
        "oauth_account_synced": account.synced,
        "campaigns": campaigns,
    }
    return data

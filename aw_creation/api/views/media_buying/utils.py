from django.http import Http404
from rest_framework.exceptions import ValidationError

from aw_creation.api.views.media_buying.constants import REPORT_CONFIG
from aw_creation.models import AccountCreation
from aw_creation.models import AdGroupCreation
from aw_creation.models import CampaignCreation
from aw_creation.models.utils import BID_STRATEGY_TYPE_MAPPING


def validate_targeting(value, valid_targeting, should_raise=True):
    errs = []
    if not isinstance(value, str):
        errs.append(f"Invalid targeting value: {value}. Must be singular string value.")
    if value not in valid_targeting:
        errs.append(f"Invalid targeting value: {value}. Valid targeting: {valid_targeting}")
    if errs:
        if should_raise:
            raise ValidationError(errs)
        config = None
    else:
        config = REPORT_CONFIG[value]
    return config


def get_account_creation(user, pk, should_raise=True):
    user = user
    try:
        account = AccountCreation.objects.user_related(user).get(pk=pk)
    except AccountCreation.DoesNotExist:
        if should_raise:
            raise Http404
        account = None
    return account


def update_or_create_campaign_creation(account_creation, campaign, params=None):
    """
    Method to handle update or creation of CampaignCreation
    If exists, then update only with supplied campaign_data
    Else, copy main settings from Campaign to create with campaign_data
    :param account_creation:
    :param campaign:
    :param params:
    :return:
    """
    params = params or {}
    try:
        campaign_creation = CampaignCreation.objects.get(account_creation=account_creation, campaign=campaign)
        {setattr(campaign_creation, key, val) for key, val in params.items()}
        campaign_creation.save()
    except CampaignCreation.DoesNotExist:
        campaign_creation = CampaignCreation.objects.create(
            account_creation=account_creation,
            campaign=campaign,
            name=campaign.name,
            start=campaign.start,
            end=campaign.end,
            type=campaign.type.upper() if campaign.type in {"Video", "Display"} else CampaignCreation.VIDEO_TYPE,
            bid_strategy_type=BID_STRATEGY_TYPE_MAPPING.get(campaign.bidding_strategy_type,
                                                            CampaignCreation.MAX_CPV_STRATEGY),
            **params
        )
    return campaign_creation


def update_or_create_ad_group_creation(campaign_creation, ad_group, params=None):
    """
    Method to handle update or creation of AdGroupCreation
    If exists, then update only with supplied ad_group_data
    Else, copy main settings from AdGroup to create with ad_group_data
    :param campaign_creation:
    :param ad_group:
    :param params:
    :return:
    """
    params = params or {}
    try:
        ad_group_creation = AdGroupCreation.objects.get(ad_group=ad_group, campaign_creation=campaign_creation)
        {setattr(ad_group_creation, key, val) for key, val in ad_group_data.items()}
        ad_group_creation.save()
    except AdGroupCreation.DoesNotExist:
        ad_group_creation = AdGroupCreation.objects.create(
            ad_group=ad_group,
            campaign_creation=campaign_creation,
            name=ad_group.name,
            **params
        )
    return ad_group_creation

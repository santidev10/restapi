import itertools

from django.db import transaction

from aw_creation.models import AdScheduleRule
from aw_creation.models import FrequencyCap
from aw_creation.models import LocationRule
from aw_creation.models import TargetingItem
from aw_reporting.models import Account
from aw_reporting.models import Opportunity
from saas import celery_app
from saas.settings import DEMO_SOURCE_ACCOUNT_ID
from .data import DEMO_ACCOUNT_ID
from .data import DEMO_BRAND
from .data import DEMO_SF_ACCOUNT
from ..models import AdGroupStatistic
from ..models import AdStatistic
from ..models import AgeRangeStatistic
from ..models import AudienceStatistic
from ..models import CampaignHourlyStatistic
from ..models import CampaignStatistic
from ..models import CityStatistic
from ..models import GenderStatistic
from ..models import KeywordStatistic
from ..models import SFAccount
from ..models import TopicStatistic
from ..models import VideoCreativeStatistic
from ..models import YTChannelStatistic
from ..models import YTVideoStatistic

__all__ = ["recreate_demo_data"]
int_iterator = itertools.count(DEMO_ACCOUNT_ID, 1)
str_iterator = (str(i) for i in int_iterator)


@celery_app.task()
def recreate_demo_data():
    with transaction.atomic():
        remove_data()
        create_data()


def remove_data():
    Opportunity.objects.filter(id=DEMO_ACCOUNT_ID).delete()
    Account.objects.filter(id=DEMO_ACCOUNT_ID).delete()


def create_data():
    clone_opportunity()
    clone_account()


def clone_opportunity():
    source_opportunity = Opportunity.objects \
        .filter(placements__adwords_campaigns__account_id=DEMO_SOURCE_ACCOUNT_ID) \
        .first()
    opportunity = clone_model(source_opportunity,
                              data=dict(id=DEMO_ACCOUNT_ID,
                                        account=SFAccount.objects.get_or_create(name=DEMO_SF_ACCOUNT)[0],
                                        brand=DEMO_BRAND))
    return opportunity


def clone_account():
    source_account = Account.objects.get(pk=DEMO_SOURCE_ACCOUNT_ID)
    account = clone_model(source_account, data=dict(id=DEMO_ACCOUNT_ID, name="Demo",
                                                    skip_creating_account_creation=True))
    clone_model(source_account.account_creation, data=dict(id=DEMO_ACCOUNT_ID, account_id=account.id))
    for index, campaign in enumerate(source_account.campaigns.all()):
        clone_campaign(campaign, account, index)
    return account


def clone_campaign(source_campaign, target_account, index):
    op_placement = clone_salesforce_placement(source_campaign.salesforce_placement)
    campaign = clone_model(source_campaign, data=dict(account_id=target_account.id, name=f"Campaign #demo{index + 1}",
                                                      salesforce_placement_id=op_placement.id))
    clone_campaign_creation(source_campaign.campaign_creation.first(), campaign, target_account.account_creation)
    for ag_index, ad_group in enumerate(source_campaign.ad_groups.all()):
        clone_ad_group(ad_group, campaign, f"{index + 1}:{ag_index + 1}")
    clone_campaign_stats(source_campaign, campaign)
    return campaign


def clone_salesforce_placement(source_placement):
    op_placement = clone_model(source_placement, data=dict(id=f"demo_{next(str_iterator)}",
                                                           opportunity_id=DEMO_ACCOUNT_ID))
    for flight in source_placement.flights.all():
        clone_model(flight, dict(id=f"demo_{next(str_iterator)}", placement_id=op_placement.id))
    return op_placement


def clone_campaign_creation(source_campaign_creation, campaign, account_creation):
    campaign_creation = clone_model(source_campaign_creation,
                                    data=dict(campaign_id=campaign.id,
                                              account_creation_id=account_creation.id,
                                              name=campaign.name))
    for language in source_campaign_creation.languages.all():
        campaign_creation.languages.add(language)
    related_models = (LocationRule, FrequencyCap, AdScheduleRule)
    for model in related_models:
        clone_bulk(model, dict(campaign_creation=source_campaign_creation),
                   dict(campaign_creation_id=campaign_creation.id))
    return campaign_creation


def clone_ad_group(source_ad_group, target_campaign, name_suffix):
    ad_group = clone_model(source_ad_group, data=dict(name=f"AdGroup #{name_suffix}", campaign_id=target_campaign.id))
    source_ad_group_creation = source_ad_group.ad_group_creation.first()
    ad_group_creation = clone_model(source_ad_group_creation,
                                    data=dict(campaign_creation_id=target_campaign.campaign_creation.first().id,
                                              ad_group_id=ad_group.id))
    clone_bulk(TargetingItem, dict(ad_group_creation=source_ad_group_creation),
               dict(ad_group_creation_id=ad_group_creation.id))
    for index, ad in enumerate(source_ad_group.ads.all()):
        clone_ad(ad, ad_group)
    clone_ad_group_stats(source_ad_group, ad_group)
    return ad_group


def clone_ad(source_ad, target_ad_group):
    ad = clone_model(source_ad, data=dict(ad_group_id=target_ad_group.id))
    clone_model(source_ad.ad_creation.first(),
                data=dict(ad_group_creation_id=target_ad_group.ad_group_creation.first().id, ad_id=ad.id))
    clone_bulk(AdStatistic, dict(ad=source_ad), dict(ad_id=ad.id))
    return ad


def clone_campaign_stats(source_campaign, campaign):
    model_classes = (CampaignStatistic, CampaignHourlyStatistic)
    for cls in model_classes:
        clone_bulk(cls, dict(campaign=source_campaign), dict(campaign_id=campaign.id))


def clone_ad_group_stats(source_ad_group, ad_group):
    model_classes = (AdGroupStatistic, GenderStatistic, AgeRangeStatistic, TopicStatistic, AudienceStatistic,
                     VideoCreativeStatistic, YTChannelStatistic, YTVideoStatistic, KeywordStatistic, CityStatistic)
    for cls in model_classes:
        clone_bulk(cls, dict(ad_group=source_ad_group), dict(ad_group_id=ad_group.id))


def clone_bulk(model, query_filter, data):
    queryset = model.objects.filter(**query_filter)
    instances = [
        clone_model(item, data=data, save=False)
        for item in queryset
    ]
    model.objects.bulk_create(instances)


def clone_model(source, data=None, fields=None, exclude_fields=("id",), save=True):
    model = type(source)
    fields = fields or [field.attname for field in model._meta.fields]
    instance_data = {
        field: getattr(source, field)
        for field in fields if field not in exclude_fields and field not in (data or dict())
    }
    new_instance = model(
        **instance_data,
        **data
    )
    if save:
        new_instance.save()
    return new_instance

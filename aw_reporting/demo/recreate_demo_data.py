import itertools
import re

from django.conf import settings
from django.db import transaction

from aw_creation.models import AdScheduleRule
from aw_creation.models import FrequencyCap
from aw_creation.models import LocationRule
from aw_creation.models import TargetingItem
from aw_reporting.models import Account
from aw_reporting.models import Opportunity
from saas import celery_app
from .data import DEMO_ACCOUNT_ID
from .data import DEMO_BRAND
from ..models import AdGroupStatistic
from ..models import AdStatistic
from ..models import AgeRangeStatistic
from ..models import AudienceStatistic
from ..models import CampaignHourlyStatistic
from ..models import CampaignStatistic
from ..models import CityStatistic
from ..models import GenderStatistic
from ..models import KeywordStatistic
from ..models import OpPlacement
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
    SFAccount.objects.filter(opportunity__id=DEMO_ACCOUNT_ID).delete()
    Opportunity.objects.filter(id=DEMO_ACCOUNT_ID).delete()
    Account.objects.filter(id=DEMO_ACCOUNT_ID).delete()


def create_data():
    clone_opportunity()
    clone_account()


def clone_opportunity():
    opportunity_number = next(opportunity_number_generator)
    opportunity_name = f"Acme Instant Coffee Q2-Q3’20 {opportunity_number}"
    source_opportunity = Opportunity.objects \
        .filter(placements__adwords_campaigns__account_id=settings.DEMO_SOURCE_ACCOUNT_ID)
    opportunities = clone_model_multiple(source_opportunity,
                                         data=dict(id=DEMO_ACCOUNT_ID,
                                                   number=opportunity_number,
                                                   name=opportunity_name,
                                                   brand=DEMO_BRAND))
    return opportunities


def clone_account():
    source_account = Account.objects.get(pk=settings.DEMO_SOURCE_ACCOUNT_ID)
    account = clone_model(source_account, data=dict(id=DEMO_ACCOUNT_ID, name="Demo",
                                                    skip_creating_account_creation=True))
    clone_model(source_account.account_creation, data=dict(id=DEMO_ACCOUNT_ID, account_id=account.id))
    for index, campaign in enumerate(source_account.campaigns.all()):
        clone_campaign(campaign, account, index)
    return account


def clone_campaign(source_campaign, target_account, index):
    op_placement = clone_salesforce_placement(source_campaign.salesforce_placement)
    placement_number = op_placement.number
    campaign_name = re.sub(r"PL\d+", placement_number, source_campaign.name)
    campaign = clone_model(source_campaign, data=dict(account_id=target_account.id, name=campaign_name,
                                                      salesforce_placement_id=op_placement.id))
    clone_model_multiple(source_campaign.campaign_creation.all(), campaign, target_account.account_creation)
    for ag_index, ad_group in enumerate(source_campaign.ad_groups.all()):
        clone_ad_group(ad_group, campaign, f"{index + 1}:{ag_index + 1}")
    clone_campaign_stats(source_campaign, campaign)
    return campaign


def clone_salesforce_placement(source_placement):
    placement_number = next(placement_number_generator)
    placement_name = re.sub(r"PL\d+", placement_number, source_placement.name)
    op_placement = clone_model(source_placement, data=dict(id=f"demo_{next(str_iterator)}",
                                                           opportunity_id=DEMO_ACCOUNT_ID,
                                                           number=placement_number,
                                                           name=placement_name))
    for flight in source_placement.flights.all():
        clone_model(flight, dict(id=f"demo_{next(str_iterator)}", placement_id=op_placement.id))
    return op_placement


def generate_salesforce_codes(prefix, Model):
    int_generator = itertools.count(1, 1)
    number_len = 6
    re_pattern = re.compile(f"^{prefix}0+$")
    while True:
        next_number = "{prefix}{code}".format(prefix=prefix,
                                              code=("0" * number_len + str(next(int_generator)))[-number_len:]
                                              )
        if re.search(re_pattern, next_number):
            number_len += 1
            continue
        if not Model.objects.filter(number=next_number).exists():
            yield next_number


placement_number_generator = generate_salesforce_codes("PL", OpPlacement)
opportunity_number_generator = generate_salesforce_codes("OP", Opportunity)


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
    source_campaign_creation = target_campaign.campaign_creation.first()
    if source_campaign_creation:
        ad_group_creation = clone_model(source_ad_group_creation,
                                        data=dict(campaign_creation_id=source_campaign_creation.id,
                                                  ad_group_id=ad_group.id))
        clone_bulk(TargetingItem, dict(ad_group_creation=source_ad_group_creation),
                   dict(ad_group_creation_id=ad_group_creation.id))
    for ad in source_ad_group.ads.all():
        clone_ad(ad, ad_group)
    clone_ad_group_stats(source_ad_group, ad_group)
    return ad_group


def clone_ad(source_ad, target_ad_group):
    ad = clone_model(source_ad, data=dict(ad_group_id=target_ad_group.id))
    if source_ad.ad_creation.exists():
        clone_model_multiple(source_ad.ad_creation.all(),
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


def clone_model_multiple(sources, data=None, fields=None, exclude_fields=("id",), save=True):
    return [
        clone_model(source, data=data, fields=fields, exclude_fields=exclude_fields, save=save)
        for source in sources
    ]

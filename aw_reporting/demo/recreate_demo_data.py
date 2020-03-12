import itertools
import random
from datetime import datetime
from datetime import time
from datetime import timedelta
from functools import partial
from itertools import product

from django.db import transaction

from aw_creation.models import AccountCreation
from aw_creation.models import AdCreation
from aw_creation.models import AdGroupCreation
from aw_creation.models import AdScheduleRule
from aw_creation.models import CampaignCreation
from aw_creation.models import FrequencyCap
from aw_creation.models import Language
from aw_creation.models import LocationRule
from aw_creation.models import TargetingItem
from aw_reporting.models import ALL_AGE_RANGES
from aw_reporting.models import ALL_DEVICES
from aw_reporting.models import ALL_GENDERS
from aw_reporting.models import Account
from aw_reporting.models import Ad
from aw_reporting.models import AdGroup
from aw_reporting.models import AdGroupStatistic
from aw_reporting.models import AdStatistic
from aw_reporting.models import AgeRangeStatistic
from aw_reporting.models import Audience
from aw_reporting.models import AudienceStatistic
from aw_reporting.models import Campaign
from aw_reporting.models import CampaignHourlyStatistic
from aw_reporting.models import CampaignStatistic
from aw_reporting.models import CampaignStatus
from aw_reporting.models import CityStatistic
from aw_reporting.models import Flight
from aw_reporting.models import GenderStatistic
from aw_reporting.models import GeoTarget
from aw_reporting.models import KeywordStatistic
from aw_reporting.models import OpPlacement
from aw_reporting.models import Opportunity
from aw_reporting.models import SFAccount
from aw_reporting.models import Topic
from aw_reporting.models import TopicStatistic
from aw_reporting.models import VideoCreative
from aw_reporting.models import VideoCreativeStatistic
from aw_reporting.models import YTChannelStatistic
from aw_reporting.models import YTVideoStatistic
from aw_reporting.models.salesforce_constants import SalesForceGoalType
from aw_reporting.update.recalculate_de_norm_fields import recalculate_de_norm_fields_for_account
from saas import celery_app
from utils.datetime import now_in_default_tz
from utils.lang import flatten
from .data import AUIDIENCES
from .data import CAMPAIGN_STATS
from .data import CHANNELS
from .data import CITIES
from .data import DAYS_LEFT
from .data import DEFAULT_CTA_STATS
from .data import DEMO_ACCOUNT_ID
from .data import DEMO_AD_GROUPS
from .data import DEMO_BRAND
from .data import DEMO_DATA_HOURLY_LIMIT
from .data import DEMO_DATA_PERIOD_DAYS
from .data import DEMO_NAME
from .data import DEMO_SF_ACCOUNT
from .data import KEYWORDS
from .data import QUARTILE_STATS
from .data import Stats
from .data import TARGETING
from .data import TOPICS
from .data import VIDEOS
from .data import VIDEO_CREATIVES

__all__ = ["recreate_demo_data"]

int_iterator = itertools.count(DEMO_ACCOUNT_ID, 1)


@celery_app.task()
def recreate_demo_data():
    with transaction.atomic():
        remove_data()
        create_data()


def remove_data():
    Opportunity.objects.filter(id=DEMO_ACCOUNT_ID).delete()
    Account.objects.filter(id=DEMO_ACCOUNT_ID).delete()


def create_data():
    dates = generate_dates()
    account = create_account()
    opportunity = create_sf_opportunity()
    campaigns = create_campaigns(account, opportunity, dates)
    create_flights(campaigns, dates)
    ad_groups = create_ad_groups(campaigns)
    ads = create_ads(ad_groups)

    create_statistic(accounts=[account], campaigns=campaigns, ad_groups=ad_groups, ads=ads, dates=dates)
    create_creation_entities(accounts=[account], campaigns=campaigns, ad_groups=ad_groups, ads=ads)


def create_account():
    account = Account(
        id=DEMO_ACCOUNT_ID,
        name=DEMO_NAME,
        skip_creating_account_creation=True,
        timezone="UTC",
    )
    account.save()
    return account


def create_sf_opportunity():
    opportunity = Opportunity.objects.create(
        id=DEMO_ACCOUNT_ID,
        account=SFAccount.objects.get_or_create(name=DEMO_SF_ACCOUNT)[0],
        brand=DEMO_BRAND,
    )
    return opportunity


def create_campaigns(account, opportunity, dates):
    start, end = min(dates), max(dates)
    campaigns = [
        Campaign(
            id=next(int_iterator),
            name="Campaign #demo{}".format(i + 1),
            account=account,
            status=CampaignStatus.SERVING.value,
            salesforce_placement=OpPlacement.objects.create(
                id=next(int_iterator),
                opportunity=opportunity,
                start=start,
                end=end,
                **stats["salesforce"]
            ),
        )
        for i, stats in enumerate(CAMPAIGN_STATS)
    ]
    Campaign.objects.bulk_create(campaigns)
    return campaigns


def create_flights(campaigns, dates):
    start, end = min(dates), max(dates)
    placements = [campaign.salesforce_placement for campaign in campaigns]
    flights = [
        Flight(
            id="{}:{}".format(DEMO_ACCOUNT_ID, i),
            placement=placement,
            total_cost=placement.total_cost,
            ordered_units=placement.ordered_units,
            start=start,
            end=end,
        )
        for i, placement in enumerate(placements)
    ]
    Flight.objects.bulk_create(flights)


def create_ad_groups(campaigns):
    ad_groups = flatten([
        generate_ad_groups(campaign)
        for campaign in campaigns
    ])
    AdGroup.objects.bulk_create(ad_groups)
    return ad_groups


def generate_ad_groups(campaign):
    return [
        AdGroup(
            id=next(int_iterator),
            name="{} #{}".format(name, campaign.id),
            campaign=campaign,
        )
        for i, name in enumerate(DEMO_AD_GROUPS)
    ]


def create_ads(ad_groups):
    ads = flatten([
        generate_ads(ad_group)
        for ad_group in ad_groups
    ])
    Ad.objects.bulk_create(ads)
    return ads


def generate_ads(ad_group):
    return [
        Ad(
            id=next(int_iterator),
            ad_group=ad_group,
        )
        for i in range(2)
    ]


def create_statistic(accounts, campaigns, ad_groups, ads, dates):
    create_campaigns_statistic(campaigns, dates)
    create_ad_groups_statistic(ad_groups, dates)
    create_ad_statistic(ads, dates)

    for account in accounts:
        recalculate_de_norm_fields_for_account(account.id)


def create_campaigns_statistic(campaigns, dates):
    create_campaigns_daily_statistic(campaigns, dates)
    create_campaigns_hourly_statistic(campaigns, dates)


def create_campaigns_daily_statistic(campaigns, dates):
    statistics = generate_campaign_statistic(campaigns=campaigns, dates=dates)
    CampaignStatistic.objects.bulk_create(statistics)


def create_campaigns_hourly_statistic(campaigns, dates):
    statistics = generate_campaign_hourly_statistic(campaigns=campaigns, dates=dates)
    CampaignHourlyStatistic.objects.bulk_create(statistics)


def generate_campaign_statistic(campaigns, dates):
    statistic_subjects = tuple(product(campaigns, dates))
    stats = generate_stats(statistic_subjects, lambda i: i[0].salesforce_placement.goal_type_id)
    return [
        CampaignStatistic(
            campaign=campaign,
            date=dt,
            impressions=impressions,
            video_views=views,
            clicks=clicks,
            cost=cost,
        )
        for (campaign, dt), impressions, views, clicks, cost in stats
    ]


def generate_campaign_hourly_statistic(campaigns, dates):
    today = now_in_default_tz().date()
    datetimes = [
        datetime.combine(dt, time(hour=hour))
        for dt, hour in product(dates, range(24))
        if dt != today or hour < DEMO_DATA_HOURLY_LIMIT
    ]
    statistic_subjects = tuple(product(campaigns, datetimes))
    stats = generate_stats(statistic_subjects, lambda i: i[0].salesforce_placement.goal_type_id)
    return [
        CampaignHourlyStatistic(
            campaign=campaign,
            date=dt.date(),
            hour=dt.hour,
            impressions=impressions,
            video_views=views,
            clicks=clicks,
            cost=cost,
        )
        for (campaign, dt), impressions, views, clicks, cost in stats
    ]


def create_ad_groups_statistic(ad_groups, dates):
    stats = (
        (AdGroupStatistic, "device_id", ALL_DEVICES, dict(average_position=1, **DEFAULT_CTA_STATS)),
        (GenderStatistic, "gender_id", ALL_GENDERS, DEFAULT_CTA_STATS),
        (AgeRangeStatistic, "age_range_id", ALL_AGE_RANGES, DEFAULT_CTA_STATS),
        (TopicStatistic, "topic", get_topics(), DEFAULT_CTA_STATS),
        (AudienceStatistic, "audience", get_audiences(), DEFAULT_CTA_STATS),
        (VideoCreativeStatistic, "creative", get_creatives(), None),
        (YTChannelStatistic, "yt_id", CHANNELS, None),
        (YTVideoStatistic, "yt_id", VIDEOS, None),
        (KeywordStatistic, "keyword", KEYWORDS, DEFAULT_CTA_STATS),
        (CityStatistic, "city", get_cities(), None),
    )

    for model, key, items, special_data in stats:
        create_ad_group_statistic_for_model(
            ad_groups=ad_groups,
            dates=dates,
            model=model,
            key=key,
            items=items,
            special_data=special_data,
        )


def create_ad_statistic(ads, dates):
    statistic_subjects = tuple(product(ads, dates))
    stats = generate_stats(
        statistic_subjects,
        lambda i: i[0].ad_group.campaign.salesforce_placement.goal_type_id,
    )
    statistics = [
        AdStatistic(
            ad=ad,
            date=dt,
            average_position=1,
            impressions=impressions,
            video_views=views,
            clicks=clicks,
            cost=cost,
            **generate_quartile_data(impressions),
        )
        for (ad, dt), impressions, views, clicks, cost in stats
    ]
    AdStatistic.objects.bulk_create(statistics)


def create_ad_group_statistic_for_model(ad_groups, dates, model, key, items, special_data):
    multiplier_per_item = {item: random.randint(1, 20) for item in items}
    statistic_subjects = tuple(product(ad_groups, dates, items))
    multiplier_for_subjects = [multiplier_per_item[item] for (*_, item) in statistic_subjects]
    stats = generate_stats(
        statistic_subjects,
        lambda i: i[0].campaign.salesforce_placement.goal_type_id,
        multiplier_for_subjects,
    )
    statistics = [
        model(
            ad_group=ad_group,
            date=dt,
            impressions=impressions,
            video_views=views,
            clicks=clicks,
            cost=cost,
            **{key: item},
            **(special_data or dict()),
            **generate_quartile_data(impressions),
        )
        for (ad_group, dt, item), impressions, views, clicks, cost in stats
    ]
    model.objects.bulk_create(statistics)


def generate_stats(statistic_subjects, goal_type_getter, multipliers=None):
    count = len(statistic_subjects)
    return zip(
        statistic_subjects,
        randomize_values(Stats.IMPRESSIONS, count, custom_multipliers=multipliers),
        randomize_values(
            Stats.VIDEO_VIEWS,
            count,
            custom_multipliers=[
                (1 if goal_type_getter(item) == SalesForceGoalType.CPV else 0)
                for item in statistic_subjects
            ]
        ),
        randomize_values(Stats.CLICKS, count, custom_multipliers=multipliers),
        randomize_values(Stats.COST, count, custom_multipliers=multipliers),
    )


def create_creation_entities(accounts, campaigns, ad_groups, ads):
    create_account_creations(accounts)
    create_campaign_creations(campaigns)
    create_ad_group_creations(ad_groups)
    create_ad_creations(ads)


def create_account_creations(accounts):
    account_creations = [
        AccountCreation(
            id=account.id,
            name=account.name,
            account=account,
        )
        for account in accounts
    ]
    AccountCreation.objects.bulk_create(account_creations)


def create_campaign_creations(campaigns):
    today = now_in_default_tz().date()
    start = today - timedelta(days=DEMO_DATA_PERIOD_DAYS)
    end = today + timedelta(days=DAYS_LEFT - 1)
    campaign_creations = [
        CampaignCreation(
            name=campaign.name,
            campaign=campaign,
            account_creation=campaign.account.account_creation,
            start=start,
            end=end,

        )
        for campaign in campaigns
    ]
    campaign_creations = CampaignCreation.objects.bulk_create(campaign_creations)
    language, _ = Language.objects.get_or_create(id=1000, defaults=dict(name="English"))
    for campaign_creation in campaign_creations:
        campaign_creation.languages.add(language)
        LocationRule.objects.create(campaign_creation=campaign_creation)
        FrequencyCap.objects.create(campaign_creation=campaign_creation, limit=5)
        AdScheduleRule.objects.create(campaign_creation=campaign_creation, day=1, from_hour=18, to_hour=23)


def create_ad_group_creations(ad_groups):
    ad_group_creations = [
        AdGroupCreation(
            name=ad_group.name,
            ad_group=ad_group,
            campaign_creation=ad_group.campaign.campaign_creation.first(),
        )
        for ad_group in ad_groups
    ]
    ad_group_creations = AdGroupCreation.objects.bulk_create(ad_group_creations)
    for ad_group_creation in ad_group_creations:
        create_targeting(ad_group_creation)


def create_targeting(ad_group_creation):
    targeting = flatten([
        [(targeting_type, item) for item in items]
        for targeting_type, items in TARGETING.items()
    ])
    for index, targeting_tuple in enumerate(targeting):
        targeting_type, targeting_data = targeting_tuple
        TargetingItem.objects.create(
            type=targeting_type.value,
            is_negative=index % 2 == 0,
            ad_group_creation=ad_group_creation,
            criteria=targeting_data["criteria"],
        )


def create_ad_creations(ads):
    ad_group_creations = [
        AdCreation(
            ad=ad,
            ad_group_creation=ad.ad_group.ad_group_creation.first(),
        )
        for ad in ads
    ]
    AdCreation.objects.bulk_create(ad_group_creations)


def get_or_create_entities(model, key, items):
    return [
        model.objects.get_or_create(**{key: item})[0]
        for item in items
    ]


def generate_dates():
    days = DEMO_DATA_PERIOD_DAYS
    today = now_in_default_tz().date()
    start = today - timedelta(days=days - 1)
    return [start + timedelta(days=i) for i in range(days)]


get_topics = partial(get_or_create_entities, Topic, "name", TOPICS)
get_audiences = partial(get_or_create_entities, Audience, "name", AUIDIENCES)
get_creatives = partial(get_or_create_entities, VideoCreative, "id", VIDEO_CREATIVES)


def get_cities():
    return [
        GeoTarget.objects.get_or_create(
            name=name,
            canonical_name=name,

        )[0]
        for name in CITIES
    ]


def generate_quartile_data(impressions):
    return {
        key: percent * impressions
        for key, percent in QUARTILE_STATS.items()
    }


def randomize_values(total_value, count, buffer=80, custom_multipliers=None):
    assert 0 <= buffer <= 100
    const_value = 100 - buffer
    if count == 0:
        return []
    custom_multipliers = custom_multipliers or (1 for _ in range(count))
    raw_numbers = [
        random.randint(const_value, 100) * m
        for m in custom_multipliers
    ]
    multiplier = total_value / sum(raw_numbers)
    return [multiplier * value for value in raw_numbers]

import itertools
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
from aw_reporting.models import ALL_AGE_RANGES, OpPlacement, Contact, SFAccount
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
from aw_reporting.models import CityStatistic
from aw_reporting.models import GenderStatistic
from aw_reporting.models import GeoTarget
from aw_reporting.models import KeywordStatistic
from aw_reporting.models import Opportunity
from aw_reporting.models import Topic
from aw_reporting.models import TopicStatistic
from aw_reporting.models import VideoCreative
from aw_reporting.models import VideoCreativeStatistic
from aw_reporting.models import YTChannelStatistic
from aw_reporting.models import YTVideoStatistic
from aw_reporting.update.recalculate_de_norm_fields import recalculate_de_norm_fields_for_account
from saas import celery_app
from utils.datetime import now_in_default_tz
from utils.lang import flatten
from .models import DEMO_ACCOUNT_ID
from .models import DEMO_AD_GROUPS
from .models import DEMO_BRAND
from .models import DEMO_CAMPAIGNS_COUNT
from .models import DEMO_DATA_HOURLY_LIMIT
from .models import DEMO_DATA_PERIOD_DAYS
from .models import DEMO_NAME
from .models import DEMO_SF_ACCOUNT

__all__ = ["recreate_demo_data"]

int_iterator = itertools.count(1, 1)


@celery_app.task()
def recreate_demo_data():
    with transaction.atomic():
        remove_data()
        create_data()


def remove_data():
    Account.objects.filter(id=DEMO_ACCOUNT_ID).delete()


def create_data():
    account = create_account()
    opportunity = create_sf_opportunity()
    campaigns = create_campaigns(account, opportunity)
    ad_groups = create_ad_groups(campaigns)
    ads = create_ads(ad_groups)

    create_statistic(accounts=[account], campaigns=campaigns, ad_groups=ad_groups, ads=ads)
    create_creation_entities(accounts=[account], campaigns=campaigns, ad_groups=ad_groups, ads=ads)


def create_account():
    account = Account(
        id=DEMO_ACCOUNT_ID,
        name=DEMO_NAME,
        skip_creating_account_creation=True,
    )
    account.save()
    return account


def create_sf_opportunity():
    opportunity = Opportunity.objects.create(
        account=SFAccount.objects.get_or_create(name=DEMO_SF_ACCOUNT)[0],
        brand=DEMO_BRAND,
    )
    return opportunity


def create_campaigns(account, opportunity):
    campaigns = [
        Campaign(
            id="demo{}".format(i + 1),
            name="Campaign #demo{}".format(i + 1),
            account=account,
            salesforce_placement=OpPlacement.objects.create(
                id=next(int_iterator),
                opportunity=opportunity,
                goal_type_id=i % 2,
                ordered_units=100000000,
                ordered_rate=0.5,
                total_cost=70000,
            ),
        )
        for i in range(DEMO_CAMPAIGNS_COUNT)
    ]
    Campaign.objects.bulk_create(campaigns)
    return campaigns


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
            id="{}{}".format(campaign.id, (i + 1)),
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
            id="{}{}".format(ad_group.id, i),
            ad_group=ad_group,
        )
        for i in range(2)
    ]


def create_statistic(accounts, campaigns, ad_groups, ads):
    dates = generate_dates()
    create_campaigns_statistic(campaigns, dates)
    create_ad_groups_statistic(ad_groups, dates)
    create_ad_statistic(ads, dates)

    for account in accounts:
        recalculate_de_norm_fields_for_account(account.id)


def create_campaigns_statistic(campaigns, dates):
    create_campaigns_daily_statistic(campaigns, dates)
    create_campaigns_hourly_statistic(campaigns, dates)


def create_campaigns_daily_statistic(campaigns, dates):
    statistics = flatten([
        generate_campaign_statistic(campaign=campaign, dates=dates)
        for campaign in campaigns
    ])
    CampaignStatistic.objects.bulk_create(statistics)


def create_campaigns_hourly_statistic(campaigns, dates):
    statistics = flatten([
        generate_campaign_hourly_statistic(campaign=campaign, dates=dates)
        for campaign in campaigns
    ])
    CampaignHourlyStatistic.objects.bulk_create(statistics)


def generate_campaign_statistic(campaign, dates):
    return [
        CampaignStatistic(
            campaign=campaign,
            date=dt,
            **DEFAULT_STATS,
        )
        for dt in dates
    ]


def generate_campaign_hourly_statistic(campaign, dates):
    today = now_in_default_tz().date()
    return [
        CampaignHourlyStatistic(
            campaign=campaign,
            date=dt,
            hour=hour,
            **DEFAULT_STATS,
        )
        for dt, hour in product(dates, range(24))
        if dt != today or hour < DEMO_DATA_HOURLY_LIMIT
    ]


def create_ad_groups_statistic(ad_groups, dates):
    stats = (
        (AdGroupStatistic, "device_id", ALL_DEVICES, dict(average_position=1)),
        (GenderStatistic, "gender_id", ALL_GENDERS, None),
        (AgeRangeStatistic, "age_range_id", ALL_AGE_RANGES, None),
        (TopicStatistic, "topic", get_topics(), None),
        (AudienceStatistic, "audience", get_audiences(), None),
        (VideoCreativeStatistic, "creative", get_creatives(), None),
        (YTChannelStatistic, "yt_id", CHANNELS, None),
        (YTVideoStatistic, "yt_id", VIDEOS, None),
        (KeywordStatistic, "keyword", KEYWORDS, None),
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
    statistics = [
        AdStatistic(
            ad=ad,
            date=dt,
            average_position=1,
            **DEFAULT_STATS,
        )
        for ad, dt in product(ads, dates)
    ]
    AdStatistic.objects.bulk_create(statistics)


def create_ad_group_statistic_for_model(ad_groups, dates, model, key, items, special_data):
    statistics = [
        model(
            ad_group=ad_group,
            date=dt,
            **{key: item},
            **DEFAULT_STATS,
            **(special_data or dict())
        )
        for ad_group, dt, item in product(ad_groups, dates, items)
    ]
    model.objects.bulk_create(statistics)


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
    campaign_creations = [
        CampaignCreation(
            name=campaign.name,
            campaign=campaign,
            account_creation=campaign.account.account_creation,
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
            type=targeting_type,
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


DEFAULT_STATS = dict(
    impressions=10000,
    cost=1,
    video_views=300,
    clicks=20,
)
TOPICS = (
    "Demo topic 1",
    "Demo topic 2",
)
AUIDIENCES = (
    "Demo audience 1",
    "Demo audience 2",
)
VIDEO_CREATIVES = (
    "demo creative 1",
    "demo creative 2",
)
KEYWORDS = (
    "demo keyword 1",
    "demo keyword 2",
)

CHANNELS = (
    "demoChannel1",
    "demoChannel2",
)

VIDEOS = (
    "demoVideo1",
    "demoVideo2",
)
CITIES = (
    "Demo city 1",
    "Demo city 2",
)
TARGETING = {
    TargetingItem.KEYWORD_TYPE: (
        {"name": "Computer & Vcriteriaeo Games", "criteria": 41},
        {"name": "Arts & Entertainment", "criteria": 3},
        {"name": "Shooter Games", "criteria": 930},
        {"name": "Movies", "criteria": 34},
        {"name": "TV Family-Oriented Shows", "criteria": 1110},
        {"name": "Business & Industrial", "criteria": 12},
        {"name": "Beauty & Fitness", "criteria": 44},
        {"name": "Food & Drink", "criteria": 71},
    ),
    TargetingItem.INTEREST_TYPE: (
        {"name": "/Beauty Mavens", "criteria": 92505},
        {"name": "/Beauty Products & Services", "criteria": 80546},
        {"name": "/Family-Focused", "criteria": 91000},
        {"name": "/News Junkies & Avcriteria Readers/Entertainment & Celebrity News Junkies", "criteria": 92006},
        {"name": "/News Junkies & Avcriteria Readers/Women\"s Media Fans", "criteria": 92007},
        {"name": "/Foodies", "criteria": 92300},
        {"name": "/Sports & Fitness/Outdoor Recreational Equipment", "criteria": 80549},
        {"name": "/Sports Fans", "criteria": 90200},
        {"name": "/News Junkies & Avcriteria Readers", "criteria": 92000},
        {"name": "/Sports & Fitness/Fitness Products & Services/Exercise Equipment", "criteria": 80559},
    ),
    TargetingItem.TOPIC_TYPE: (
        {"name": "Computer & Vcriteriaeo Games", "criteria": 41},
        {"name": "Arts & Entertainment", "criteria": 3},
        {"name": "Shooter Games", "criteria": 930},
        {"name": "Movies", "criteria": 34},
        {"name": "TV Family-Oriented Shows", "criteria": 1110},
        {"name": "Business & Industrial", "criteria": 12},
        {"name": "Beauty & Fitness", "criteria": 44},
        {"name": "Food & Drink", "criteria": 71},
    ),
}

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

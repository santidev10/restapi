from aw_creation.constants import TargetingType
from aw_reporting.models.salesforce_constants import SalesForceGoalType

DEMO_ACCOUNT_ID = 9000000
DEMO_DATA_PERIOD_DAYS = 19
DAYS_LEFT = 11
DEMO_AD_GROUPS = (
    "Topics", "Interests", "Keywords", "Channels", "Videos"
)
DEMO_BRAND = "Acme CO"
DEMO_COST_METHOD = ["CPM", "CPV"]
DEMO_DATA_HOURLY_LIMIT = 13

CAMPAIGN_NAME_REPLACEMENTS = dict((
    ("DSK - YT_TrV_PT PL018076 Female Creative #2", "DSK - YT_TrV_PT PL0 Neutral Creative"),
    ("DSK - YT_TrV_PT PL018076 Male Creative #2", "DSK - YT_TrV_PT PL0 Neutral Creative"),
    ("MOB - YT_TrV_PT PL018076 Female Creative #2", "MOB - YT_TrV_PT PL0 Neutral Creative"),
    ("MOB - YT_TrV_PT PL018076 Male Creative #2", "MOB - YT_TrV_PT PL0 Neutral Creative"),
))


class Stats:
    IMPRESSIONS = 300000000
    VIDEO_VIEWS = 10000000
    CLICKS = 150000
    COST = 37000


CAMPAIGN_STATS = (
    dict(salesforce=dict(
        goal_type_id=SalesForceGoalType.CPV,
        ordered_units=21600000,
        ordered_rate=.23,
        total_cost=4968000,
    )),
    dict(salesforce=dict(
        goal_type_id=SalesForceGoalType.CPM,
        ordered_units=242600000,
        ordered_rate=11.,
        total_cost=2668600,
    )),
)

QUARTILE_STATS = dict(
    video_views_25_quartile=.60,
    video_views_50_quartile=.40,
    video_views_75_quartile=.34,
    video_views_100_quartile=.27,
)
DEFAULT_CTA_STATS = dict(
    clicks_call_to_action_overlay=10,
    clicks_website=20,
    clicks_app_store=30,
    clicks_cards=40,
    clicks_end_cap=50,
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
    "XEngrJr79Jg",
    "TvAGSnbK5kI",
)
KEYWORDS = (
    "demo keyword 1",
    "demo keyword 2",
)
CHANNELS = (
    "UC-NAeWIiaJSX0lQ20IVEFIA",
    "UCW0ecgZ5CXuvRh059T6HDFQ",
)
VIDEOS = (
    "XEngrJr79Jg",
    "TvAGSnbK5kI",
)
CITIES = (
    "Demo city 1",
    "Demo city 2",
)
TARGETING = {
    TargetingType.KEYWORD: (
        {"name": "Computer & Vcriteriaeo Games", "criteria": 41},
        {"name": "Arts & Entertainment", "criteria": 3},
        {"name": "Shooter Games", "criteria": 930},
        {"name": "Movies", "criteria": 34},
        {"name": "TV Family-Oriented Shows", "criteria": 1110},
        {"name": "Business & Industrial", "criteria": 12},
        {"name": "Beauty & Fitness", "criteria": 44},
        {"name": "Food & Drink", "criteria": 71},
    ),
    TargetingType.INTEREST: (
        {"name": "/Beauty Mavens", "criteria": 92505},
        {"name": "/Beauty Products & Services", "criteria": 80546},
        {"name": "/Family-Focused", "criteria": 91000},
        {"name": "/News Junkies & Avcriteria Readers/Entertainment & Celebrity News Junkies", "criteria": 92006},
        {"name": "/News Junkies & Avid Readers/Women's Media Fans", "criteria": 92007},
        {"name": "/Foodies", "criteria": 92300},
        {"name": "/Sports & Fitness/Outdoor Recreational Equipment", "criteria": 80549},
        {"name": "/Sports Fans", "criteria": 90200},
        {"name": "/News Junkies & Avcriteria Readers", "criteria": 92000},
        {"name": "/Sports & Fitness/Fitness Products & Services/Exercise Equipment", "criteria": 80559},
    ),
    TargetingType.TOPIC: (
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

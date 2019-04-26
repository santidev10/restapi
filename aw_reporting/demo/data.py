from aw_creation.constants import TargetingType

DEMO_ACCOUNT_ID = "demo"
DEMO_NAME = "Demo"
DEMO_CAMPAIGNS_COUNT = 2
DEMO_DATA_PERIOD_DAYS = 20
DEMO_AD_GROUPS = (
    "Topics", "Interests", "Keywords", "Channels", "Videos"
)
DEMO_BRAND = "Demo Brand"
DEMO_COST_METHOD = ["CPM", "CPV"]
DEMO_SF_ACCOUNT = "Initiative LA"
DEMO_DATA_HOURLY_LIMIT = 13
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
        {"name": "/News Junkies & Avcriteria Readers/Women\"s Media Fans", "criteria": 92007},
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

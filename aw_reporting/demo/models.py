import logging
from datetime import datetime, timedelta
from aw_reporting.models import *
# pylint: disable=import-error
from singledb.connector import SingleDatabaseApiConnector, \
    SingleDatabaseApiConnectorException
# pylint: enable=import-error

logger = logging.getLogger(__name__)

DEMO_ACCOUNT_ID = "demo"
DEMO_CAMPAIGNS_COUNT = 2
DEMO_AD_GROUPS = (
    "Topics", "Interests", "Keywords", "Channels", "Videos"
)
TOTAL_DEMO_AD_GROUPS_COUNT = len(DEMO_AD_GROUPS) * DEMO_CAMPAIGNS_COUNT

IMPRESSIONS = 150000
VIDEO_VIEWS = 53000
CLICKS = 1500
COST = 3700
ALL_CONVERSIONS = 15000
CONVERSIONS = 9000
VIEW_THROUGH = 4000


class BaseDemo:
    id = DEMO_ACCOUNT_ID
    period_proportion = 1
    week_proportion = 0.2
    last_week_proportions = 0.15
    name = "Demo"
    status = 'eligible'
    children = []
    parent = None

    average_position = 1
    ad_network = "YouTube Videos"

    video25rate = 60
    video50rate = 40
    video75rate = 34
    video100rate = 27

    optimization_impressions_value = 20
    optimization_video_views_value = 5
    optimization_clicks_value = 2
    optimization_cost_value = 5
    optimization_ctr_value = 0.998
    optimization_average_cpv_value = 0.0699
    optimization_average_cpm_value = 24.68
    optimization_video_view_rate_value = 5
    optimization_conversions_value = 5
    optimization_view_through_value = 5

    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)

        self.now = datetime.now()
        self.today = self.now.date()
        self.yesterday = datetime.now().date()
        self.start_date = self.today - timedelta(days=19)
        self.end_date = self.today + timedelta(days=10)
        self._channels = None
        self._videos = None

    def __getitem__(self, name):
        return getattr(self, name)

    video_criteria = dict(
        is_safe=True,
        lang_code='en',
        country="United States",
    )
    channel_criteria = dict(
        country="United States",
        is_content_safe=True,
    )

    def get_channels(self):
        if self._channels is None:
            connector = SingleDatabaseApiConnector()
            try:
                items = connector.get_custom_query_result(
                    model_name="channel",
                    fields=["id", "title", "thumbnail_image_url"],
                    limit=12,
                    order_by="-details__subscribers",
                    **self.channel_criteria
                )
            except SingleDatabaseApiConnectorException as e:
                logger.error(e)
                items = []
            self._channels = items
        return self._channels

    def get_videos(self):
        if self._videos is None:
            connector = SingleDatabaseApiConnector()
            try:
                items = connector.get_custom_query_result(
                    model_name="video",
                    fields=["id", "title", "thumbnail_image_url", "duration"],
                    limit=12,
                    order_by="-views",
                    **self.video_criteria
                )
            except SingleDatabaseApiConnectorException as e:
                logger.error(e)
                items = []
            self._videos = items
        return self._videos

    @property
    def channel(self):
        channel = [
            dict(
                id=i['id'],
                label=i['title'],
                thumbnail=i['thumbnail_image_url'],
            )
            for i in self.get_channels()
            ]
        return channel

    @property
    def video(self):
        video = [
            dict(
                id=i['id'],
                label=i['title'],
                thumbnail=i['thumbnail_image_url'],
                duration=i.get("duration"),
            )
            for i in self.get_videos()[:6]
            ]
        return video

    @property
    def creative(self):
        creative = [
            dict(
                id=i['id'],
                label=i['title'],
                thumbnail=i['thumbnail_image_url'],
                duration=i.get("duration"),
            )
            for i in self.get_videos()[6:12]
            ]
        return creative

    @property
    def ad(self):
        ads = []

        def get_ads(ad_groups):
            for ag in ad_groups:
                ads.append(
                    dict(
                        label="Ad #{}".format(ag.id),
                        average_position=1,
                        status="eligible",
                    )
                )

        if isinstance(self, DemoAccount):
            for c in self.children:
                get_ads(c.children)
        else:
            get_ads(self.children)

        return ads

    topic = (
        {'label': 'Computer & Video Games', 'id': 41},
        {'label': 'Arts & Entertainment', 'id': 3},
        {'label': 'Shooter Games', "id": 930},
        {'label': 'Movies', "id": 34},
        {'label': 'TV Family-Oriented Shows', "id": 1110},
        {'label': 'Business & Industrial', "id": 12},
        {'label': 'Beauty & Fitness', "id": 44},
        {'label': 'Food & Drink', "id": 71},
    )

    interest = (
        {'label': '/Beauty Mavens', "id": 92505},
        {'label': '/Beauty Products & Services', "id": 80546},
        {'label': '/Family-Focused', "id": 91000},
        {'label': '/News Junkies & Avid Readers/Entertainment '
                  '& Celebrity News Junkies',
         'id': 92006},
        {'label': "/News Junkies & Avid Readers/Women's Media Fans",
         'id': 92007},
        {'label': '/Foodies', 'id': 92300},
        {'label': '/Sports & Fitness/Outdoor Recreational Equipment',
         'id': 80549},
        {'label': '/Sports Fans', "id": 90200},
        {'label': '/News Junkies & Avid Readers', "id": 92000},
        {'label': '/Sports & Fitness/Fitness Products & Services/'
                  'Exercise Equipment', "id": 80559},
    )

    remarketing = (
        {'label': 'Visited Channel Page'},
        {'label': 'Viewed Any Video Via Ad'},
        {'label': 'Subscribed'},
    )

    keyword = (
        {'label': 'fitness videos'},
        {'label': 'beauty tutorial'},
        {'label': 'food'},
        {'label': 'music'},
        {'label': 'entertainment'},
        {'label': 'cardio workout'},
        {'label': 'cardio exercise workout'},
        {'label': 'fitness workouts and exercises'},
    )

    location = (
        {'label': 'New York,New York'},
        {'label': 'Los Angeles,California'},
        {'label': 'Houston,Texas'},
        {'label': 'Dallas,Texas'},
        {'label': 'Chicago,Illinois'},
        {'label': 'Atlanta,Georgia'},
        {'label': 'Miami,Florida'},
        {'label': 'Philadelphia,Pennsylvania'},
        {'label': 'Phoenix,Arizona'},
        {'label': 'San Antonio,Texas'},
    )

    @property
    def device(self):
        return tuple({'label': d} for d in Devices)

    @property
    def gender(self):
        return tuple({'label': d} for d in Genders)

    @property
    def age(self):
        ranges = AgeRanges[1:] + AgeRanges[0:1]
        return tuple({'label': d} for d in ranges)

    @property
    def impressions(self):
        return sum(i.impressions for i in self.children)

    @property
    def video_impressions(self):
        return self.impressions

    @property
    def impressions_this_week(self):
        return sum(i.impressions_this_week for i in self.children)

    @property
    def impressions_last_week(self):
        return sum(i.impressions_last_week for i in self.children)

    @property
    def video_views(self):
        return sum(i.video_views for i in self.children)

    @property
    def video_views_this_week(self):
        return sum(i.video_views_this_week for i in self.children)

    @property
    def video_views_last_week(self):
        return sum(i.video_views_last_week for i in self.children)

    @property
    def clicks(self):
        return sum(i.clicks for i in self.children)

    @property
    def clicks_this_week(self):
        return sum(i.clicks_this_week for i in self.children)

    @property
    def clicks_last_week(self):
        return sum(i.clicks_last_week for i in self.children)

    @property
    def cost(self):
        return sum(i.cost for i in self.children)

    @property
    def cost_this_week(self):
        return sum(i.cost_this_week for i in self.children)

    @property
    def cost_last_week(self):
        return sum(i.cost_last_week for i in self.children)

    @property
    def all_conversions(self):
        return sum(i.all_conversions for i in self.children)

    @property
    def conversions(self):
        return sum(i.conversions for i in self.children)

    @property
    def view_through(self):
        return sum(i.view_through for i in self.children)

    @property
    def average_cpm(self):
        return self.cost / self.impressions * 1000 \
            if self.impressions else None

    @property
    def average_cpv(self):
        return self.cost / self.video_views if self.video_views else None

    @property
    def average_cpv_top(self):
        return self.cost / self.video_views * 1.2 if self.video_views else None

    @property
    def average_cpv_bottom(self):
        return self.cost / self.video_views * 0.8 if self.video_views else None

    @property
    def video_view_rate(self):
        return self.video_views / self.impressions * 100 \
            if self.impressions else None

    @property
    def video_view_rate_top(self):
        return self.video_views / self.impressions * 100 * 1.2 \
            if self.impressions else None

    @property
    def video_view_rate_bottom(self):
        return self.video_views / self.impressions * 100 * 0.8 \
            if self.impressions else None

    @property
    def ctr(self):
        return self.clicks / self.impressions * 100 \
            if self.impressions else None

    @property
    def ctr_top(self):
        return self.clicks / self.impressions * 100 * 1.2 \
            if self.impressions else None

    @property
    def ctr_bottom(self):
        return self.clicks / self.impressions * 100 * 0.8 \
            if self.impressions else None

    @property
    def ctr_v(self):
        return self.clicks / self.video_views * 100 \
            if self.video_views else None

    @property
    def ctr_v_top(self):
        return self.clicks / self.video_views * 100 * 1.2 \
            if self.video_views else None

    @property
    def ctr_v_bottom(self):
        return self.clicks / self.video_views * 100 * 0.8 \
            if self.video_views else None

class DemoAd(BaseDemo):

    @property
    def creation_details(self):
        data = dict(
            id=self.id,
            name=self.name,
            updated_at=self.now,
            display_url="www.channelfactory.com",
            thumbnail="https://i.ytimg.com/vi/XEngrJr79Jg/hqdefault.jpg",
            final_url="https://www.channelfactory.com",
            video_url="https://www.youtube.com/watch?v=XEngrJr79Jg",
            tracking_template="https://www.custom_tracking_service.us/?ad=XEngrr79Jg",
            custom_params=[{"name": "ad", "value": "demo_ad"},
                           {"name": "provider", "value": "ad_words"}],
            video_id="XEngrJr79Jg",
            video_title="Channel Factory Social Video Marketing",
            video_channel_title="Channel Factory",
            video_description="Channel Factory is a pioneer in the native advertising ecosystem, building a leading Global Next-Generation Media Company. In this new world of fragmented marketing opportunities, we use data and technology to provide a unified solution.",
            companion_banner=None,
            video_thumbnail="http://img.youtube.com/vi/XEngrJr79Jg/hqdefault.jpg",
        )
        return data


class DemoAdGroup(BaseDemo):

    def __init__(self, **kwargs):
        super(DemoAdGroup, self).__init__(**kwargs)
        self.children = [
            DemoAd(id="{}".format(self.id),
                   name="Demo ad #{}".format(self.id),
                   parent=self)
        ]

    items_proportion = 1 / TOTAL_DEMO_AD_GROUPS_COUNT

    @property
    def impressions(self):
        return int(IMPRESSIONS * self.items_proportion * self.period_proportion)

    @property
    def impressions_this_week(self):
        return int(IMPRESSIONS * self.items_proportion * self.week_proportion)

    @property
    def impressions_last_week(self):
        return int(IMPRESSIONS * self.items_proportion * self.last_week_proportions)

    @property
    def video_views(self):
        return int(VIDEO_VIEWS * self.items_proportion * self.period_proportion)

    @property
    def video_views_this_week(self):
        return int(VIDEO_VIEWS * self.items_proportion * self.week_proportion)

    @property
    def video_views_last_week(self):
        return int(VIDEO_VIEWS * self.items_proportion * self.last_week_proportions)

    @property
    def clicks(self):
        return int(CLICKS * self.items_proportion * self.period_proportion)

    @property
    def clicks_this_week(self):
        return int(CLICKS * self.items_proportion * self.week_proportion)

    @property
    def clicks_last_week(self):
        return int(CLICKS * self.items_proportion * self.last_week_proportions)

    @property
    def cost(self):
        return int(COST * self.items_proportion * self.period_proportion)

    @property
    def cost_this_week(self):
        return int(COST * self.items_proportion * self.week_proportion)

    @property
    def cost_last_week(self):
        return int(COST * self.items_proportion * self.last_week_proportions)

    @property
    def all_conversions(self):
        return int(ALL_CONVERSIONS * self.items_proportion * self.period_proportion)

    @property
    def conversions(self):
        return int(CONVERSIONS * self.items_proportion * self.period_proportion)

    @property
    def view_through(self):
        return int(VIEW_THROUGH * self.items_proportion * self.period_proportion)

    def get_targeting_list(self, list_type, sub_list_type=None):
        from aw_creation.models import TargetingItem
        items = []
        if list_type == TargetingItem.VIDEO_TYPE:
            items = [
                dict(
                    criteria=i['id'],
                    is_negative=bool(n % 2),
                    type=TargetingItem.VIDEO_TYPE,
                    name=i['label'],
                    id=i['id'],
                    thumbnail=i['thumbnail'],
                )
                for n, i in enumerate(self.parent.parent.video)
            ]
        elif list_type == TargetingItem.CHANNEL_TYPE:
            items = [
                dict(
                    criteria=i['id'],
                    is_negative=bool(n % 2),
                    type=TargetingItem.CHANNEL_TYPE,
                    name=i['label'],
                    id=i['id'],
                    thumbnail=i['thumbnail'],
                )
                for n, i in enumerate(self.parent.parent.channel)
            ]
        elif list_type == TargetingItem.TOPIC_TYPE:
            items = [
                dict(
                    criteria=i['id'],
                    is_negative=bool(n % 2),
                    type=TargetingItem.TOPIC_TYPE,
                    name=i['label'],
                )
                for n, i in enumerate(self.topic)
            ]
        elif list_type == TargetingItem.INTEREST_TYPE:
            items = [
                dict(
                    criteria=i['id'],
                    is_negative=bool(n % 2),
                    type=TargetingItem.INTEREST_TYPE,
                    name=i['label'],
                )
                for n, i in enumerate(self.interest)
            ]
        elif list_type == TargetingItem.KEYWORD_TYPE:
            items = [
                dict(
                    criteria=i['label'],
                    is_negative=bool(n % 2),
                    type=TargetingItem.KEYWORD_TYPE,
                    name=i['label'],
                )
                for n, i in enumerate(self.keyword)
            ]
        if sub_list_type:
            is_negative = sub_list_type == "negative"
            items = list(filter(lambda i: i["is_negative"] == is_negative, items))
        return items

    @property
    def creation_details(self):
        from aw_creation.models import AdGroupCreation, TargetingItem

        targeting = {}
        for t, _ in TargetingItem.TYPES:
            items = self.get_targeting_list(t)
            positive = []
            negative = []
            for item in self.get_targeting_list(t):
                if item['is_negative']:
                    negative.append(item)
                else:
                    positive.append(item)
            targeting[t] = {"positive": positive, "negative": negative}


        data = dict(
            id=self.id,
            name=self.name,
            max_rate=0.07,
            updated_at=self.now,
            ad_creations=[i.creation_details for i in self.children],
            targeting=targeting,
            age_ranges=[
                dict(id=uid, name=n)
                for uid, n in AdGroupCreation.AGE_RANGES
            ],
            genders=[
                dict(id=uid, name=n)
                for uid, n in AdGroupCreation.GENDERS
            ],
            parents=[
                dict(id=uid, name=n)
                for uid, n in AdGroupCreation.PARENTS
            ],
        )
        return data


class DemoCampaign(BaseDemo):
    type = "Video"
    budget = 100

    @property
    def name(self):
        return "{} #{}".format(self.__class__.__name__[4:], self.id)

    def __init__(self, **kwargs):
        super(DemoCampaign, self).__init__(**kwargs)
        self.children = [
            DemoAdGroup(id="{}{}".format(self.id, (i + 1)),
                        name="{} #{}".format(name, self.id),
                        parent=self)
            for i, name in enumerate(DEMO_AD_GROUPS)
            ]

    @property
    def creation_details(self):
        from aw_creation.models import LocationRule, FrequencyCap, \
            CampaignCreation
        data = dict(
            id=self.id,
            name=self.name,
            updated_at=self.now,
            budget=self.budget,
            devices=[
                dict(id=d, name=n) for d, n in CampaignCreation.DEVICES
            ],
            location_rules=[
                dict(
                    longitude="-118.20533000",
                    latitude="33.99711660",
                    radius=145,
                    bid_modifier=1,
                    radius_units=dict(id=LocationRule.UNITS[0][0],
                                      name=LocationRule.UNITS[0][1]),
                    geo_target=None,
                )
            ],
            languages=[dict(id=1000, name="English")],
            ad_group_creations=[
                a.creation_details
                for a in self.children
            ],
            frequency_capping=[
                dict(
                    event_type=dict(id=FrequencyCap.EVENT_TYPES[0][0],
                                    name=FrequencyCap.EVENT_TYPES[0][1]),
                    limit=5,
                    level=dict(id=FrequencyCap.LEVELS[0][0],
                               name=FrequencyCap.LEVELS[0][1]),
                    time_unit=dict(id=FrequencyCap.TIME_UNITS[0][0],
                                   name=FrequencyCap.TIME_UNITS[0][1]),
                )
            ],
            ad_schedule_rules=[
                dict(
                    from_hour=18,
                    from_minute=0,
                    to_minute=0,
                    to_hour=23,
                    day=i,
                    campaign_creation=self.id,
                ) for i in range(1, 8)
            ],
            start=self.start_date,
            end=self.end_date,

            video_ad_format=dict(
                id=CampaignCreation.VIDEO_AD_FORMATS[0][0],
                name=CampaignCreation.VIDEO_AD_FORMATS[0][1],
            ),
            delivery_method=dict(
                id=CampaignCreation.DELIVERY_METHODS[0][0],
                name=CampaignCreation.DELIVERY_METHODS[0][1],
            ),
            video_networks=[
                dict(id=uid, name=name)
                for uid, name in CampaignCreation.VIDEO_NETWORKS
            ],
            age_ranges=[
                dict(id=uid, name=n)
                for uid, n in CampaignCreation.AGE_RANGES
            ],
            genders=[
                dict(id=uid, name=n)
                for uid, n in CampaignCreation.GENDERS
            ],
            parents=[
                dict(id=uid, name=n)
                for uid, n in CampaignCreation.PARENTS
            ],
            content_exclusions=[
                dict(id=uid, name=n)
                for uid, n in CampaignCreation.CONTENT_LABELS[5:7]
            ],
        )
        return data


class DemoAccount(BaseDemo):
    def __init__(self, **kwargs):
        super(DemoAccount, self).__init__(**kwargs)
        self.children = [DemoCampaign(id="demo{}".format(i + 1),
                                      parent=self)
                         for i in range(DEMO_CAMPAIGNS_COUNT)]

    def filter_out_items(self, campaigns, ad_groups):
        if ad_groups:
            new_campaigns = []
            for c in self.children:
                new_groups = [a for a in c.children if a.id in ad_groups]
                if new_groups:
                    c.children = new_groups
                    new_campaigns.append(c)
            self.children = new_campaigns

        elif campaigns:
            self.children = [c for c in self.children if c.id in campaigns]

    def set_period_proportion(self, start_date, end_date):
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, DATE_FORMAT).date()
        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, DATE_FORMAT).date()

        start_date = start_date or self.start_date
        start_date = min(start_date, self.yesterday)

        end_date = end_date or self.end_date
        end_date = min(end_date, self.yesterday)

        week = (datetime.now().date() - start_date).days + 1

        selected = (end_date - start_date).days + 1
        total_days = (self.yesterday - self.start_date).days + 1
        period_proportion = selected / total_days
        week_proportion = 7 / total_days if week > 7 else week / total_days

        for c in self.children:
            for a in c.children:
                a.period_proportion = period_proportion
                a.week_proportion = week_proportion

    @property
    def details(self):
        from aw_reporting.demo.charts import DemoChart

        details = dict(
            age=[dict(name=e, value=i + 1)
                 for i, e in enumerate(reversed(AgeRanges))],
            gender=[dict(name=e, value=i + 1)
                    for i, e in enumerate(Genders)],
            device=[dict(name=e, value=i + 1)
                    for i, e in enumerate(reversed(Devices))],
            average_position=self.average_position,
            video100rate=self.video100rate,
            video25rate=self.video25rate,
            video50rate=self.video50rate,
            video75rate=self.video75rate,
            conversions=self.conversions,
            all_conversions=self.all_conversions,
            view_through=self.view_through,
            creative=[dict(id=i['id'], name=i['label'], thumbnail=i['thumbnail'])
                      for i in self.creative],

            delivery_trend=[]
        )
        for indicator in ("impressions", "video_views"):
            filters = dict(
                start_date=None,
                end_date=None,
                indicator=indicator,
            )
            charts_obj = DemoChart(self, filters)
            chart_lines = charts_obj.chart_lines(self, filters)
            details['delivery_trend'].append(
                dict(label=indicator.split("_")[-1].capitalize(),
                     trend=chart_lines[0]['trend']))

        return details

    @property
    def overview(self):
        data = dict(
            age=[dict(name=e, value=i + 1)
                 for i, e in enumerate(reversed(AgeRanges))],
            gender=[dict(name=e, value=i + 1)
                    for i, e in enumerate(Genders)],
            device=[dict(name=e, value=i + 1)
                    for i, e in enumerate(reversed(Devices))],
            location=[dict(name=e['label'], value=i + 1)
                    for i, e in enumerate(reversed(self.location))][:6],
            clicks=self.clicks,
            clicks_this_week=self.clicks_this_week,
            clicks_last_week=self.clicks_last_week,
            cost=self.cost,
            cost_this_week=self.cost_this_week,
            cost_last_week=self.cost_last_week,
            impressions=self.impressions,
            impressions_this_week=self.impressions_this_week,
            impressions_last_week=self.impressions_last_week,
            video_views=self.video_views,
            video_views_this_week=self.video_views_this_week,
            video_views_last_week=self.video_views_last_week,
            ctr=self.ctr,
            ctr_top=self.ctr_top,
            ctr_bottom=self.ctr_bottom,
            ctr_v=self.ctr_v,
            ctr_v_top=self.ctr_v_top,
            ctr_v_bottom=self.ctr_v_bottom,
            average_cpm=self.average_cpm,
            average_cpv=self.average_cpv,
            average_cpv_top=self.average_cpv_top,
            average_cpv_bottom=self.average_cpv_bottom,
            video_view_rate=self.video_view_rate,
            video_view_rate_top=self.video_view_rate_top,
            video_view_rate_bottom=self.video_view_rate_bottom,
            video100rate=self.video100rate,
            video25rate=self.video25rate,
            video50rate=self.video50rate,
            video75rate=self.video75rate,
            conversions=self.conversions,
            all_conversions=self.all_conversions,
            view_through=self.view_through,
        )
        return data

    @property
    def header_data(self):
        from aw_reporting.demo.charts import DemoChart
        filters = dict(
            start_date=self.today - timedelta(days=7),
            end_date=self.today - timedelta(days=1),
            indicator="video_views",
        )
        new_demo = DemoAccount()
        new_demo.set_period_proportion(filters['start_date'],
                                   filters['end_date'])
        charts_obj = DemoChart(new_demo, filters)
        chart_lines = charts_obj.chart_lines(new_demo, filters)

        data = dict(
            id=self.id,
            account=self.id,
            thumbnail="https://i.ytimg.com/vi/XEngrJr79Jg/hqdefault.jpg",
            name=self.name,
            status="Running",
            start=self.start_date,
            end=self.end_date,
            is_changed=False,
            is_managed=True,
            weekly_chart=chart_lines[0]['trend'],
            video_view_rate=self.video_view_rate,
            impressions=self.impressions,
            video_views=self.video_views,
            cost=self.cost,
            clicks=self.clicks,
            ctr_v=self.ctr_v,
        )
        return data

    @property
    def account_details(self):  # TODO: remove this after we get rid of Track page
        from aw_reporting.demo.charts import DemoChart
        filters = dict(
            start_date=self.today - timedelta(days=7),
            end_date=self.today - timedelta(days=1),
            indicator="video_views",
        )
        new_demo = DemoAccount()
        new_demo.set_period_proportion(filters['start_date'],
                                       filters['end_date'])
        charts_obj = DemoChart(new_demo, filters)
        chart_lines = charts_obj.chart_lines(new_demo, filters)

        data = dict(
            id=self.id,
            name=self.name,
            account_creation=self.id,
            end=self.end_date,
            start=self.start_date,
            status="Eligible",
            weekly_chart=chart_lines[0]['trend'],
            clicks=self.clicks,
            cost=self.cost,
            impressions=self.impressions,
            video_views=self.video_views,
            video_view_rate=self.video_view_rate,
            ctr_v=self.ctr_v,
        )
        return data

    @property
    def creation_details(self):
        from aw_creation.models import AccountCreation
        from aw_reporting.demo.charts import DemoChart

        creative = self.creative
        if creative:
            creative = dict(id=creative[0]['id'],
                            name=creative[0]['label'],
                            thumbnail=creative[0]['thumbnail'])

        filters = dict(
            start_date=self.start_date,
            end_date=self.end_date,
            indicator="video_views",
        )
        charts_obj = DemoChart(
            self, filters, summary_label="AW", goal_units=VIDEO_VIEWS,
            cumulative=True,
        )

        data = self.header_data
        data.update(
            creative=creative,
            structure=[
                dict(
                    id=c.id,
                    name=c.name,
                    ad_group_creations=[
                        dict(id=a.id, name=a.name)
                        for a in c.children
                    ]
                )
                for c in self.children
            ],
            is_ended=False,
            is_paused=False,
            is_approved=True,
            goal_charts=charts_obj.chart_lines(self, filters),
        )
        return data


    @property
    def creation_details_full(self):
        data = dict(
            id=self.id,
            account=self.id,
            name=self.name,
            updated_at=self.now,
            is_ended=False,
            is_paused=False,
            is_approved=True,
            campaign_creations=[
                c.creation_details
                for c in self.children
            ],
        )
        return data

    def account_passes_filters(self, filters):
        search = filters.get('search')
        if search and search not in self.name:
            return

        status = filters.get('status')
        if status and status != "Running":
            return

        min_goal_units = filters.get('min_goal_units')
        if min_goal_units and int(min_goal_units) > VIDEO_VIEWS:
            return

        max_goal_units = filters.get('max_goal_units')
        if max_goal_units and int(max_goal_units) < VIDEO_VIEWS:
            return

        min_campaigns_count = filters.get('min_campaigns_count')
        if min_campaigns_count and int(min_campaigns_count) > DEMO_CAMPAIGNS_COUNT:
            return

        max_campaigns_count = filters.get('max_campaigns_count')
        if max_campaigns_count and int(max_campaigns_count) < DEMO_CAMPAIGNS_COUNT:
            return

        min_start = filters.get('min_start')
        if min_start and min_start > str(self.start_date):
            return

        max_start = filters.get('max_start')
        if max_start and max_start < str(self.start_date):
            return

        min_end = filters.get('min_end')
        if min_end and min_end > str(self.end_date):
            return

        max_end = filters.get('max_end')
        if max_end and max_end < str(self.end_date):
            return

        is_changed = filters.get('is_changed')
        if is_changed:
            if int(is_changed):
                return

        for metric in ("impressions", "video_views", "clicks", "cost", "video_view_rate", "ctr_v"):
            for is_max, option in enumerate(("min", "max")):
                filter_value = filters.get("{}_{}".format(option, metric))
                if filter_value:
                    filter_value = float(filter_value)
                    demo_value =  getattr(self, metric)
                    if is_max:
                        if demo_value > filter_value:
                            return
                    else:
                        if demo_value < filter_value:
                            return
        return True
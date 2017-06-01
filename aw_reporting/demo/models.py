import logging
from datetime import datetime, timedelta
from aw_reporting.models import *
from singledb.connector import SingleDatabaseApiConnector, \
    SingleDatabaseApiConnectorException

logger = logging.getLogger(__name__)

DEMO_ACCOUNT_ID = "demo"
DEMO_CAMPAIGNS_COUNT = 2
DEMO_AD_GROUPS = (
    "Topics", "Interests", "Keywords", "Channels", "Videos"
)
TOTAL_DEMO_AD_GROUPS_COUNT = len(DEMO_AD_GROUPS) * DEMO_CAMPAIGNS_COUNT

IMPRESSIONS = 15000
VIDEO_VIEWS = 5300
CLICKS = 150
COST = 370
ALL_CONVERSIONS = 1500
CONVERSIONS = 900
VIEW_THROUGH = 400


class BaseDemo:
    id = DEMO_ACCOUNT_ID
    period_proportion = 1
    name = "Demo"
    status = 'enabled'
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
    optimization_ctr_value = 1.0
    optimization_average_cpv_value = 0.065
    optimization_average_cpm_value = 23.5
    optimization_video_view_rate_value = 5
    optimization_conversions_value = 5
    optimization_view_through_value = 5

    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)

        self.today = datetime.now().date()
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
                    fields=["id", "title", "thumbnail_image_url"],
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
                        status="enabled",
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
    def video_views(self):
        return sum(i.video_views for i in self.children)

    @property
    def clicks(self):
        return sum(i.clicks for i in self.children)

    @property
    def cost(self):
        return sum(i.cost for i in self.children)

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
    def video_view_rate(self):
        return self.video_views / self.impressions * 100 \
            if self.impressions else None

    @property
    def ctr(self):
        return self.clicks / self.impressions * 100 \
            if self.impressions else None

    @property
    def ctr_v(self):
        return self.clicks / self.video_views * 100 \
            if self.video_views else None


class DemoAdGroup(BaseDemo):
    items_proportion = 1 / TOTAL_DEMO_AD_GROUPS_COUNT

    @property
    def impressions(self):
        return int(IMPRESSIONS * self.items_proportion * self.period_proportion)

    @property
    def video_views(self):
        return int(VIDEO_VIEWS * self.items_proportion * self.period_proportion)

    @property
    def clicks(self):
        return int(CLICKS * self.items_proportion * self.period_proportion)

    @property
    def cost(self):
        return int(COST * self.items_proportion * self.period_proportion)

    @property
    def all_conversions(self):
        return int(ALL_CONVERSIONS * self.items_proportion * self.period_proportion)

    @property
    def conversions(self):
        return int(CONVERSIONS * self.items_proportion * self.period_proportion)

    @property
    def view_through(self):
        return int(VIEW_THROUGH * self.items_proportion * self.period_proportion)

    @property
    def creation_details(self):
        from aw_creation.models import AdGroupCreation, TargetingItem
        data = dict(
            id=self.id,
            name=self.name,
            ct_overlay_text="Demo overlay text",
            parents=[
                dict(id=uid, name=n)
                for uid, n in AdGroupCreation.PARENTS
            ],
            targeting=dict(
                video=[
                    dict(
                        criteria=i['id'],
                        is_negative=bool(n % 2),
                        type=TargetingItem.VIDEO_TYPE,
                        name=i['label'],
                        id=i['id'],
                        thumbnail=i['thumbnail'],
                    )
                    for n, i in enumerate(self.parent.parent.video)
                ],
                topic=[
                    dict(
                        criteria=i['id'],
                        is_negative=bool(n % 2),
                        type=TargetingItem.TOPIC_TYPE,
                        name=i['label'],
                    )
                    for n, i in enumerate(self.topic)
                ],
                channel=[
                    dict(
                        criteria=i['id'],
                        is_negative=bool(n % 2),
                        type=TargetingItem.CHANNEL_TYPE,
                        name=i['label'],
                        id=i['id'],
                        thumbnail=i['thumbnail'],
                    )
                    for n, i in enumerate(self.parent.parent.channel)
                ],
                interest=[
                    dict(
                        criteria=i['id'],
                        is_negative=bool(n % 2),
                        type=TargetingItem.INTEREST_TYPE,
                        name=i['label'],
                    )
                    for n, i in enumerate(self.interest)
                ],
                keyword=[
                    dict(
                        criteria=i['label'],
                        is_negative=bool(n % 2),
                        type=TargetingItem.KEYWORD_TYPE,
                        name=i['label'],
                    )
                    for n, i in enumerate(self.keyword)
                ],
            ),
            is_approved=True,
            age_ranges=[
                dict(id=uid, name=n)
                for uid, n in AdGroupCreation.AGE_RANGES
            ],
            display_url="www.channelfactory.com",
            thumbnail="https://i.ytimg.com/vi/XEngrJr79Jg/hqdefault.jpg",
            genders=[
                dict(id=uid, name=n)
                for uid, n in AdGroupCreation.GENDERS
            ],
            max_rate=0.1,
            final_url="https://www.channelfactory.com",
            video_url="https://www.youtube.com/watch?v=XEngrJr79Jg",
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
            goal_units=VIDEO_VIEWS / DEMO_CAMPAIGNS_COUNT,
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
            max_rate=0.1,
            is_paused=False,
            is_approved=True,
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
        )
        return data


class DemoAccount(BaseDemo):

    def __init__(self, **kwargs):
        super(DemoAccount, self).__init__(**kwargs)
        self.children = [DemoCampaign(id=str(i + 1), parent=self)
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
        start_date = start_date or self.start_date
        start_date = min(start_date, self.yesterday)

        end_date = end_date or self.end_date
        end_date = min(end_date, self.yesterday)

        selected = (end_date - start_date).days + 1
        total_days = (self.yesterday - self.start_date).days + 1
        period_proportion = selected / total_days
        for c in self.children:
            for a in c.children:
                a.period_proportion = period_proportion

    @property
    def details(self):

        channels = []
        all_channels = self.channel[:3]
        for i in all_channels:
            channel = dict(
                id=i['id'],
                name=i['label'],
                thumbnail=i['thumbnail'],
                cost=self.cost / len(all_channels),
                impressions=self.impressions // len(all_channels),
                video_views=self.video_views // len(all_channels),
                clicks=self.clicks // len(all_channels),
            )
            dict_add_calculated_stats(channel)
            channels.append(channel)

        videos = []
        all_videos = self.video[:3]
        for i in all_videos:
            video = dict(
                id=i['id'],
                name=i['label'],
                thumbnail=i['thumbnail'],
                cost=self.cost / len(all_channels),
                impressions=self.impressions // len(all_channels),
                video_views=self.video_views // len(all_channels),
                clicks=self.clicks // len(all_channels),
            )
            dict_add_calculated_stats(video)
            videos.append(video)

        creative_list = []
        all_creative = self.creative[:3]
        for i in all_creative:
            video = dict(
                id=i['id'],
                name=i['label'],
                thumbnail=i['thumbnail'],
                cost=self.cost / len(all_channels),
                impressions=self.impressions // len(all_channels),
                video_views=self.video_views // len(all_channels),
                clicks=self.clicks // len(all_channels),
            )
            dict_add_calculated_stats(video)
            creative_list.append(video)

        details = dict(
            id=self.id,
            name=self.name,
            start_date=self.start_date,
            end_date=self.end_date,
            age=[dict(name=e, value=i+1)
                 for i, e in enumerate(reversed(AgeRanges))],
            gender=[dict(name=e, value=i+1)
                    for i, e in enumerate(Genders)],
            device=[dict(name=e, value=i+1)
                    for i, e in enumerate(reversed(Devices))],
            channel=channels,
            creative=creative_list,
            video=videos,
            clicks=self.clicks,
            cost=self.cost,
            impressions=self.impressions,
            video_views=self.video_views,
            ctr=self.ctr,
            ctr_v=self.ctr_v,
            average_cpm=self.average_cpm,
            average_cpv=self.average_cpv,
            video_view_rate=self.video_view_rate,
            average_position=self.average_position,
            ad_network=self.ad_network,
            video100rate=self.video100rate,
            video25rate=self.video25rate,
            video50rate=self.video50rate,
            video75rate=self.video75rate,
            conversions=self.conversions,
            all_conversions=self.all_conversions,
            view_through=self.view_through,
        )
        return details

    @property
    def creation_details(self):
        from aw_creation.models import AccountCreation
        from aw_reporting.demo.charts import DemoChart

        creative = self.creative
        if creative:
            creative = dict(id=creative[0]['id'],
                            name=creative[0]['label'],
                            thumbnail=creative[0]['thumbnail'])

        demo_details = dict(
            id=self.id,
            name=self.name,
            status="Running",
            impressions=self.impressions,
            start=self.start_date,
            end=self.end_date,
            views=self.video_views,
            cost=self.cost,
            campaigns_count=len(self.children),
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
            video_ad_format=dict(
                id=AccountCreation.VIDEO_AD_FORMATS[0][0],
                name=AccountCreation.VIDEO_AD_FORMATS[0][1],
            ),
            goal_type=dict(
                id=AccountCreation.GOAL_TYPES[0][0],
                name=AccountCreation.GOAL_TYPES[0][1],
            ),
            delivery_method=dict(
                id=AccountCreation.DELIVERY_METHODS[0][0],
                name=AccountCreation.DELIVERY_METHODS[0][1],
            ),
            video_networks=[
                dict(id=uid, name=name)
                for uid, name in AccountCreation.VIDEO_NETWORKS
            ],
            type=dict(
                id=AccountCreation.CAMPAIGN_TYPES[0][0],
                name=AccountCreation.CAMPAIGN_TYPES[0][1],
            ),
            bidding_type=dict(
                id=AccountCreation.BIDDING_TYPES[0][0],
                name=AccountCreation.BIDDING_TYPES[0][1],
            ),
            is_optimization_active=True,
            is_changed=False,
        )
        #
        filters = dict(
            start_date=self.start_date,
            end_date=self.end_date,
            indicator="video_views",
        )
        charts_obj = DemoChart(
            self, filters, summary_label="AW", goal_units=VIDEO_VIEWS,
            cumulative=True,
        )
        demo_details['goal_charts'] = charts_obj.chart_lines(self, filters)

        # weekly chart
        filters = dict(
            start_date=self.today - timedelta(days=7),
            end_date=self.today - timedelta(days=1),
            indicator="video_views",
        )
        self.set_period_proportion(filters['start_date'],
                                   filters['end_date'])
        charts_obj = DemoChart(self, filters)
        chart_lines = charts_obj.chart_lines(self, filters)
        demo_details['weekly_chart'] = chart_lines[0]['trend']
        return demo_details

    @property
    def creation_details_full(self):
        data = self.creation_details
        data.update(
            is_ended=False,
            is_paused=False,
            is_approved=True,
            budget=sum(
                c.budget
                for c in self.children
            ),
            campaign_creations=[
                c.creation_details
                for c in self.children
            ],
        )
        return data

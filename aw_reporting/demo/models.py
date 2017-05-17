from datetime import datetime, timedelta

from aw_reporting.models import *

DEMO_ACCOUNT_ID = "demo"
DEMO_CAMPAIGNS_COUNT = 2
DEMO_AD_GROUPS = (
    "Topics", "Interests", "Keywords", "Channels", "Videos"
)
TOTAL_DEMO_AD_GROUPS_COUNT = len(DEMO_AD_GROUPS) * DEMO_CAMPAIGNS_COUNT

IMPRESSIONS = 15000
VIDEO_VIEWS = 5300
CLICKS = 3400
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

    average_position = 1
    ad_network = "YouTube Videos"

    video25rate = 60
    video50rate = 40
    video75rate = 34
    video100rate = 27

    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)

        self.today = datetime.now().date()
        self.yesterday = datetime.now().date()
        self.start_date = self.today - timedelta(days=19)
        self.end_date = self.today + timedelta(days=10)

    def __getitem__(self, name):
        return getattr(self, name)

    default_creative = (
        {
            'label': 'Nauru Files (Australia) - BBC News',
            'id': None,
            'thumbnail': 'https://i.ytimg.com/vi/R39ghkO69fI/hqdefault.jpg'
        },
        {
            'label': 'Anna van der Breggen - BBC Sport interview',
            'id': None,
            'thumbnail': 'https://i.ytimg.com/vi/DEA6AxrNWUA/hqdefault.jpg'
        },
        {
            'label': 'The Hunt - EP 7 TRAILER',
            'id': None,
            'thumbnail': 'https://i.ytimg.com/vi/qmQmwEkK65U/hqdefault.jpg'
        },
    )

    default_channel = (
        {
            'label': 'SciShow Space',
            'id': None,
            'thumbnail': 'https://yt3.ggpht.com/-TA--KN7t7dQ/AAAAAAAAAAI/AAAAAAAAAAA/04IfrZ3uZvQ/s88-c-k-no-mo-rj-c0xffffff/photo.jpg',
        },
        {
            'label': 'thebrainscoop',
            'id': None,
            'thumbnail': 'https://yt3.ggpht.com/-PHvFjeB5gJg/AAAAAAAAAAI/AAAAAAAAAAA/06Gf-ATVD6I/s88-c-k-no-mo-rj-c0xffffff/photo.jpg',
        },
        {
            'label': 'CGP Grey',
            'id': None,
            'thumbnail': 'https://yt3.ggpht.com/-hcwBgBwDiuk/AAAAAAAAAAI/AAAAAAAAAAA/eQENCQCzV4w/s88-c-k-no-rj-c0xffffff/photo.jpg',
        },
        {
            'label': 'CrashCourse',
            'id': None,
            'thumbnail': 'https://yt3.ggpht.com/-CbyerLDiZAc/AAAAAAAAAAI/AAAAAAAAAAA/XOz_qJn17K0/s88-c-k-no-rj-c0xffffff/photo.jpg',
        },
        {
            'label': 'MinuteEarth',
            'id': None,
            'thumbnail': 'https://yt3.ggpht.com/-4N4bJGqwS94/AAAAAAAAAAI/AAAAAAAAAAA/VRmzCPV9CXw/s88-c-k-no-rj-c0xffffff/photo.jpg',
        },
        {
            'label': 'MinutePhysics',
            'id': None,
            'thumbnail': 'https://i.ytimg.com/i/UHW94eEFW7hkUMVaZz4eDg/1.jpg',
        },
    )

    default_video = (
        {
            'label': 'How to upload YouTube videos by default (very useful 2016)',
            'id': None,
            'thumbnail': 'https://i.ytimg.com/vi/Zuj9nKZJLf4/hqdefault.jpg',
        },
        {
            'label': 'How to give the BEST PowerPoint presentation!',
            'id': None,
            'thumbnail': 'https://i.ytimg.com/vi/eHhqWbI0y4M/hqdefault.jpg',
        },
        {
            'label': 'Vocabulary & Expressions for POKER and other card games',
            'id': None,
            'thumbnail': 'https://i.ytimg.com/vi/r1gfifW5F0U/hqdefault.jpg',
        },
        {
            'label': 'Learn the FUTURE PROGRESSIVE TENSE in English',
            'id': None,
            'thumbnail': 'https://i.ytimg.com/vi/N6ejjMWsFfg/hqdefault.jpg',
        },
        {
            'label': '15 Fishy Expressions in English',
            'id': None,
            'thumbnail': 'https://i.ytimg.com/vi/l9oqWH_9_N0/hqdefault.jpg',
        },
        {
            'label': 'Internet Safety',
            'id': None,
            'thumbnail': 'https://i.ytimg.com/vi/N0xiAFsa_aE/hqdefault.jpg',
        },
    )

    video_criteria = dict(
        is_safe=True,
        lang_code='en',
    )

    @property
    def channel(self):
        return []
        from video.models import Video, Channel

        channel_ids = Video.objects.filter(
            **self.video_criteria
        ).values_list('channel_id', flat=True).distinct()[:6]
        channel = [
            dict(
                label=v['title'], id=v['id'],
                thumbnail=v['thumbnail_image_url']
            )
            for v in Channel.objects.filter(id__in=channel_ids).values(
                'id', 'title', 'thumbnail_image_url'
            )
        ]
        if not channel:
            return self.default_channel

        return channel

    @property
    def video(self):
        return []
        from video.models import Video
        videos = [
            dict(label=v.title, id=v.id, thumbnail=v.thumbnail_image_url)
            for v in Video.objects.filter(**self.video_criteria)[:6]
        ]
        if not videos:
            return self.default_video
        return videos

    @property
    def creative(self):
        return []
        from video.models import Video
        video = [
            dict(label=v.title, id=v.id, thumbnail=v.thumbnail_image_url)
            for v in Video.objects.filter(**self.video_criteria)[6:12]
        ]
        if not video:
            return self.default_creative
        return video

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
        {'label': 'Computer & Video Games'},
        {'label': 'Arts & Entertainment'},
        {'label': 'Shooter Games'},
        {'label': 'Movies'},
        {'label': 'TV Family-Oriented Shows'},
        {'label': 'Business & Industrial'},
        {'label': 'Beauty & Fitness'},
        {'label': 'Food & Drink'},
    )

    interest = (
        {'label': '/Beauty Mavens'},
        {'label': '/Beauty Products & Services'},
        {'label': '/Family-Focused'},
        {'label': '/News Junkies & Avid Readers/Entertainment '
                  '& Celebrity News Junkies'},
        {'label': "/News Junkies & Avid Readers/Women's Media Fans"},
        {'label': '/Foodies'},
        {'label': '/Sports & Fitness/Outdoor Recreational Equipment'},
        {'label': '/Sports Fans'},
        {'label': '/News Junkies & Avid Readers'},
        {'label': '/Sports & Fitness/Fitness Products & Services/'
                  'Exercise Equipment'},
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


class DemoCampaign(BaseDemo):
    type = "Video"

    @property
    def name(self):
        return "{} #{}".format(self.__class__.__name__[4:], self.id)

    def __init__(self, **kwargs):
        super(DemoCampaign, self).__init__(**kwargs)
        self.children = [
            DemoAdGroup(id="{}{}".format(self.id, (i + 1)),
                        name="{} #{}".format(name, self.id))
            for i, name in enumerate(DEMO_AD_GROUPS)
        ]


class DemoAccount(BaseDemo):

    def __init__(self, **kwargs):
        super(DemoAccount, self).__init__(**kwargs)
        self.children = [DemoCampaign(id=str(i + 1))
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
            channel=[],
            creative=[],
            video=[],
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

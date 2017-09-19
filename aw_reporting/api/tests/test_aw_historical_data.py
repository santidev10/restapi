from django.core.urlresolvers import reverse
from django.contrib.auth import get_user_model
from django.utils import timezone
from rest_framework.status import HTTP_200_OK, HTTP_400_BAD_REQUEST
from aw_reporting.demo.models import *
from .base import AwReportingAPITestCase
from aw_reporting.utils import get_dates_range


class AwHistoricalDataAPITestCase(AwReportingAPITestCase):
    keys = {
        'clicks', 'impressions', 'video_views',
        'ctr', 'ctr_v', 'video_view_rate', 'average_cpv',

        'video_views_this_week', 'video_views_last_week',
        'clicks_this_week', 'clicks_last_week',
        'impressions_this_week', 'impressions_last_week',

        'video_view_rate_top', 'video_view_rate_bottom',
        'ctr_v_top', 'ctr_v_bottom', 'ctr_bottom', 'ctr_top',
        'average_cpv_top', 'average_cpv_bottom',
    }

    def setUp(self):
        self.user = self.create_test_user()

    def test_wrong_item_type(self):
        channel_id = "ab123ff"
        url = reverse("aw_reporting_urls:aw_historical_data",
                      args=("jedi", channel_id))
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_success_get_channel_stats(self):
        account = self.create_account(self.user)
        stats = dict(
            impressions=4, video_views=2, clicks=1, cost=1,
            video_views_25_quartile=4, video_views_50_quartile=3,
            video_views_75_quartile=2, video_views_100_quartile=1,
        )
        campaign = Campaign.objects.create(id=1, name="", account=account)
        ad_group = AdGroup.objects.create(id=1, name="", campaign=campaign)
        yesterday = datetime.now().date() - timedelta(days=1)
        start = yesterday - timedelta(days=13)
        channel_id = "ab123ff"
        for date in get_dates_range(start, yesterday):
            YTChannelStatistic.objects.create(yt_id=channel_id, ad_group=ad_group, date=date, **stats)

        url = reverse("aw_reporting_urls:aw_historical_data",
                      args=("channel", channel_id))
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        self.assertEqual(set(data.keys()), self.keys)

    def test_success_get_video_stats(self):
        account = self.create_account(self.user)
        stats = dict(
            impressions=4, video_views=2, clicks=1, cost=1,
            video_views_25_quartile=4, video_views_50_quartile=3,
            video_views_75_quartile=2, video_views_100_quartile=1,
        )
        campaign = Campaign.objects.create(id=1, name="", account=account)
        ad_group = AdGroup.objects.create(id=1, name="", campaign=campaign)
        yesterday = datetime.now().date() - timedelta(days=1)
        start = yesterday - timedelta(days=13)
        channel_id = "ab123ff"
        for date in get_dates_range(start, yesterday):
            YTVideoStatistic.objects.create(yt_id=channel_id, ad_group=ad_group, date=date, **stats)

        url = reverse("aw_reporting_urls:aw_historical_data",
                      args=("video", channel_id))
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        self.assertEqual(set(data.keys()), self.keys)

    def test_success_get_video_stats_not_owner(self):
        another_user = get_user_model().objects.create(email="wtf@m.ua")
        account = self.create_account(another_user)
        stats = dict(
            impressions=4, video_views=2, clicks=1, cost=1,
            video_views_25_quartile=4, video_views_50_quartile=3,
            video_views_75_quartile=2, video_views_100_quartile=1,
        )
        campaign = Campaign.objects.create(id=1, name="", account=account)
        ad_group = AdGroup.objects.create(id=1, name="", campaign=campaign)
        yesterday = datetime.now().date() - timedelta(days=1)
        start = yesterday - timedelta(days=13)
        channel_id = "ab123ff"
        for date in get_dates_range(start, yesterday):
            YTVideoStatistic.objects.create(yt_id=channel_id, ad_group=ad_group, date=date, **stats)

        url = reverse("aw_reporting_urls:aw_historical_data",
                      args=("video", channel_id))
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        self.assertEqual(data['clicks'], None)
        self.assertEqual(data['video_views'], None)
        self.assertEqual(data['impressions'], None)

    def test_success_get_cf_channel_stats(self):
        account = Account.objects.create(id="1", name="CF customer account")
        manager = Account.objects.create(id="3386233102", name="Promopushmaster")
        account.managers.add(manager)
        stats = dict(
            impressions=4, video_views=2, clicks=1, cost=1,
            video_views_25_quartile=4, video_views_50_quartile=3,
            video_views_75_quartile=2, video_views_100_quartile=1,
        )
        campaign = Campaign.objects.create(id=1, name="", account=account)
        ad_group = AdGroup.objects.create(id=1, name="", campaign=campaign)
        yesterday = timezone.now().date() - timedelta(days=1)
        start = yesterday - timedelta(days=13)
        channel_id = "ab123ff"
        for date in get_dates_range(start, yesterday):
            YTChannelStatistic.objects.create(yt_id=channel_id, ad_group=ad_group, date=date, **stats)

        url = reverse("aw_reporting_urls:aw_historical_data",
                      args=("channel", channel_id))
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        self.assertEqual(set(data.keys()), self.keys)
        days = (yesterday - start).days + 1
        self.assertEqual(data['impressions'], stats['impressions'] * days)

    def test_success_get_cf_video_stats(self):
        account = Account.objects.create(id="1", name="CF customer account")
        manager = Account.objects.create(id="3386233102", name="Promopushmaster")
        account.managers.add(manager)
        stats = dict(
            impressions=4, video_views=2, clicks=1, cost=1,
            video_views_25_quartile=4, video_views_50_quartile=3,
            video_views_75_quartile=2, video_views_100_quartile=1,
        )
        campaign = Campaign.objects.create(id=1, name="", account=account)
        ad_group = AdGroup.objects.create(id=1, name="", campaign=campaign)
        yesterday = datetime.now().date() - timedelta(days=1)
        start = yesterday - timedelta(days=13)
        channel_id = "ab123ff"
        for date in get_dates_range(start, yesterday):
            YTVideoStatistic.objects.create(yt_id=channel_id, ad_group=ad_group, date=date, **stats)

        url = reverse("aw_reporting_urls:aw_historical_data",
                      args=("video", channel_id))
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        self.assertEqual(set(data.keys()), self.keys)
        days = (yesterday - start).days + 1
        self.assertEqual(data['impressions'], stats['impressions'] * days)

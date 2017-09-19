import json
from unittest.mock import patch
from django.core.urlresolvers import reverse
from django.http import StreamingHttpResponse
from rest_framework.status import HTTP_200_OK
from datetime import datetime, timedelta
from aw_reporting.demo.models import DEMO_ACCOUNT_ID
from aw_creation.models import AccountCreation
from aw_reporting.models import Account, Campaign, AdGroup, AdGroupStatistic, GenderStatistic, AgeRangeStatistic, \
    AudienceStatistic, VideoCreativeStatistic, YTVideoStatistic, YTChannelStatistic, TopicStatistic, \
    KeywordStatistic, CityStatistic, AdStatistic, VideoCreative, GeoTarget, Audience, Topic, Ad, \
    AWConnectionToUserRelation, AWConnection
from saas.utils_tests import SingleDatabaseApiConnectorPatcher
from saas.utils_tests import ExtendedAPITestCase


class PerformanceExportAPITestCase(ExtendedAPITestCase):

    @staticmethod
    def create_stats(account):
        campaign1 = Campaign.objects.create(id=1, name="#1", account=account)
        ad_group1 = AdGroup.objects.create(id=1, name="", campaign=campaign1)
        campaign2 = Campaign.objects.create(id=2, name="#2", account=account)
        ad_group2 = AdGroup.objects.create(id=2, name="", campaign=campaign2)
        date = datetime.now().date() - timedelta(days=1)
        base_stats = dict(date=date, impressions=100, video_views=10, cost=1)
        topic, _ = Topic.objects.get_or_create(id=1, defaults=dict(name="boo"))
        audience, _ = Audience.objects.get_or_create(id=1, defaults=dict(name="boo", type="A"))
        creative, _ = VideoCreative.objects.get_or_create(id=1)
        city, _ = GeoTarget.objects.get_or_create(id=1, defaults=dict(name="bobruisk"))
        ad = Ad.objects.create(id=1, ad_group=ad_group1)
        AdStatistic.objects.create(ad=ad, average_position=1, **base_stats)

        for ad_group in (ad_group1, ad_group2):
            stats = dict(ad_group=ad_group, **base_stats)
            AdGroupStatistic.objects.create(average_position=1, **stats)
            GenderStatistic.objects.create(**stats)
            AgeRangeStatistic.objects.create(**stats)
            TopicStatistic.objects.create(topic=topic, **stats)
            AudienceStatistic.objects.create(audience=audience, **stats)
            VideoCreativeStatistic.objects.create(creative=creative, **stats)
            YTChannelStatistic.objects.create(yt_id="123", **stats)
            YTVideoStatistic.objects.create(yt_id="123", **stats)
            KeywordStatistic.objects.create(keyword="123", **stats)
            CityStatistic.objects.create(city=city, **stats)

    def test_success(self):
        user = self.create_test_user()
        AWConnectionToUserRelation.objects.create(  # user must have a connected account not to see demo data
            connection=AWConnection.objects.create(email="me@mail.kz", refresh_token=""),
            user=user,
        )
        account = Account.objects.create(id=1, name="")
        account_creation = AccountCreation.objects.create(name="", owner=user, is_managed=False, account=account)
        self.create_stats(account)

        url = reverse("aw_creation_urls:performance_export",
                      args=(account_creation.id,))
        today = datetime.now().date()
        filters = {
            'start_date': str(today - timedelta(days=1)),
            'end_date': str(today),
        }
        with patch("aw_reporting.charts.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher):
            response = self.client.post(
                url, json.dumps(filters), content_type='application/json',
            )
            self.assertEqual(response.status_code, HTTP_200_OK)
            self.assertEqual(type(response), StreamingHttpResponse)
            self.assertGreater(len(list(response)), 10)

    def test_success_demo(self):
        self.create_test_user()
        url = reverse("aw_creation_urls:performance_export",
                      args=(DEMO_ACCOUNT_ID,))
        today = datetime.now().date()
        filters = {
            'start_date': str(today - timedelta(days=1)),
            'end_date': str(today),
        }
        with patch("aw_reporting.demo.models.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher):
            response = self.client.post(
                url, json.dumps(filters), content_type='application/json',
            )
            self.assertEqual(response.status_code, HTTP_200_OK)
            self.assertEqual(type(response), StreamingHttpResponse)
            self.assertGreater(len(list(response)), 10)

    def test_success_demo_data(self):
        user = self.create_test_user()
        account_creation = AccountCreation.objects.create(name="", owner=user)
        url = reverse("aw_creation_urls:performance_export",
                      args=(account_creation.id,))
        today = datetime.now().date()
        filters = {
            'start_date': str(today - timedelta(days=1)),
            'end_date': str(today),
        }
        with patch("aw_reporting.demo.models.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher):
            response = self.client.post(
                url, json.dumps(filters), content_type='application/json',
            )
            self.assertEqual(response.status_code, HTTP_200_OK)
            self.assertEqual(type(response), StreamingHttpResponse)
            self.assertGreater(len(list(response)), 10)


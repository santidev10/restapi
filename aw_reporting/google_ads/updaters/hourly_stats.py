from datetime import datetime
from datetime import timedelta

from django.db import transaction
import pytz

from aw_reporting.google_ads import constants
from aw_reporting.google_ads.update_mixin import UpdateMixin
from aw_reporting.models import CampaignHourlyStatistic
from aw_reporting.models import Campaign
from aw_reporting.models import ACTION_STATUSES


class HourlyStatUpdater(UpdateMixin):
    RESOURCE_NAME = "campaign"

    def __init__(self, account):
        self.client = None
        self.ga_service = None
        self.campaign_status_enum = None
        self.advertising_type_enum = None
        self.campaign_serving_status_enum = None
        self.account = account
        self.today = datetime.now(tz=pytz.timezone(account.timezone)).date()
        self.existing_statistics = CampaignHourlyStatistic.objects.filter(campaign__account=account)
        self.existing_campaign_ids = set(self.account.campaigns.values_list("id", flat=True))

    def update(self, client):
        self.client = client
        self.ga_service = client.get_service("GoogleAdsService", version="v2")
        self.campaign_status_enum = self.client.get_type("CampaignStatusEnum", version="v2").CampaignStatus
        self.campaign_serving_status_enum = self.client.get_type("CampaignServingStatusEnum",
                                                                 version="v2").CampaignServingStatus
        self.advertising_type_enum = self.client.get_type("AdvertisingChannelTypeEnum",
                                                          version="v2").AdvertisingChannelType

        min_date = self.today - timedelta(days=10)
        last_statistic = self.existing_statistics.filter(date__lt=min_date).order_by("-date").first()
        if last_statistic:
            start_date = last_statistic.date
        else:
            start_date = min_date
        hourly_performance = self._get_hourly_campaign_performance(start_date, self.today)
        self.existing_statistics.filter(date__gte=start_date).delete()
        self._create_instances(hourly_performance, start_date)

    def _get_hourly_campaign_performance(self, min_date, max_date):
        """
        Retrieve campaign hourly performance
        :param min_date: str -> 2012-01-01
        :param max_date: str -> 2012-12-31
        :return: Google Ads search response
        """
        query_fields = self.format_query(constants.CAMPAIGN_HOURLY_STATS_PERFORMANCE_FIELDS)
        query = f"SELECT {query_fields} FROM {self.RESOURCE_NAME} WHERE segments.date BETWEEN '{min_date}' AND '{max_date}'"
        hourly_performance = self.ga_service.search(self.account.id, query=query)
        return hourly_performance

    def _create_instances(self, hourly_performance: iter, start_date):
        """
        Create new campaigns and campaign statistics
        :param hourly_performance: Google ads campaign resource search response
        :return:
        """
        campaigns_to_create = []
        campaign_hourly_stat_to_create = []
        for row in hourly_performance:
            campaign_id = str(row.campaign.id.value)
            if campaign_id not in self.existing_campaign_ids:
                self.existing_campaign_ids.add(campaign_id)
                campaigns_to_create.append(self._instantiate_campaign(campaign_id, row))
            campaign_hourly_stat_to_create.append(self._instantiate_campaign_hourly_stat(row))

        with transaction.atomic():
            Campaign.objects.bulk_create(campaigns_to_create)
            # Drop statistics
            self.existing_statistics.filter(date__gte=start_date).delete()
            CampaignHourlyStatistic.objects.bulk_create(campaign_hourly_stat_to_create)

    def _instantiate_campaign(self, campaign_id, row):
        try:
            end_date = datetime.strptime(row.campaign.end_date.value, constants.DATE_YMD)
        except AttributeError:
            end_date = None
        campaign_status = self.campaign_status_enum.Name(row.campaign.status)
        serving_status = self.campaign_serving_status_enum.Name(row.campaign.serving_status)

        to_create = Campaign(
            id=campaign_id,
            name=row.campaign.name.value,
            account=self.account,
            type=self.advertising_type_enum.Name(row.campaign.advertising_channel_type),
            start_date=row.campaign.start_date.value,
            end_date=end_date,
            budget=float(row.campaign_budget.amount_micros.value) / 1000000,
            status=campaign_status if campaign_status in ACTION_STATUSES else serving_status,
            impressions=1,  # to show this item on the accounts lists Track/Filters
        )
        return to_create

    def _instantiate_campaign_hourly_stat(self, row):
        to_create = CampaignHourlyStatistic(
            date=row.segments.date.value,
            hour=row.segments.hour.value,
            campaign_id=str(row.campaign.id.value),
            video_views=row.metrics.video_views.value,
            impressions=row.metrics.impressions.value,
            clicks=row.metrics.clicks.value,
            cost=float(row.campaign_budget.amount_micros.value) / 1000000,
        )
        return to_create

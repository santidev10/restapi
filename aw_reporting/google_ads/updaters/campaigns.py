from datetime import datetime
from datetime import timedelta

from django.db.models import Max
from django.utils import timezone
import logging
import pytz

from aw_reporting.google_ads import constants
from aw_reporting.google_ads.update_mixin import UpdateMixin
from aw_reporting.models import Account
from aw_reporting.models import ACTION_STATUSES
from aw_reporting.models import Campaign
from aw_reporting.models import CampaignHourlyStatistic
from aw_reporting.models import CampaignStatistic
from aw_reporting.models.ad_words.constants import BudgetType
from utils.datetime import now_in_default_tz

logger = logging.getLogger(__name__)


class CampaignUpdater(UpdateMixin):
    RESOURCE_NAME = "campaign"

    def __init__(self, account):
        self.account = account
        self.today = datetime.now(tz=pytz.timezone(account.timezone)).date()
        self.existing_campaigns = set()
        self.existing_statistics = CampaignStatistic.objects.filter(campaign__account=account)
        self.existing_hourly_statistics = CampaignHourlyStatistic.objects.filter(campaign__account=account)
        # Will be set by update method
        self.client = None
        self.ga_service = None
        self.channel_type_enum = None
        self.campaign_status_enum = None
        self.campaign_serving_status_enum = None

    def update(self, client):
        self.client = client
        self.ga_service = client.get_service("GoogleAdsService", version="v2")
        self.channel_type_enum = self.client.get_type("AdvertisingChannelTypeEnum", version="v2").AdvertisingChannelType
        self.campaign_status_enum = self.client.get_type("CampaignStatusEnum", version="v2").CampaignStatus
        self.campaign_serving_status_enum = self.client.get_type("CampaignServingStatusEnum",
                                                                 version="v2").CampaignServingStatus

        # Campaign performance segmented by date and all_conversions
        campaign_performance, click_type_data = self._get_campaign_performance()
        self._create_instances(campaign_performance, click_type_data)

        # Campaign performance segmented by hour
        campaign_hourly_performance = self._get_campaign_hourly_performance()
        self._create_hourly_instances(campaign_hourly_performance)

        # Update account
        Account.objects.filter(id=self.account.id).update(hourly_updated_at=timezone.now())

    def _get_campaign_performance(self):
        """
        Retrieve campaign performance
        :return: Google ads campaign resource search response
        """
        # Find min and max dates
        now = now_in_default_tz()
        max_date = self.max_ready_date(now, tz_str=self.account.timezone)

        dates = self.existing_statistics.aggregate(max_date=Max("date"))
        min_date = dates["max_date"] + timedelta(days=1) if dates["max_date"] else constants.MIN_FETCH_DATE

        click_type_data = self.get_clicks_report(
            self.client, self.ga_service, self.account,
            min_date, max_date,
            resource_name=self.RESOURCE_NAME
        )
        # Delete stale data
        self.drop_latest_stats(self.existing_statistics, self.today)
        campaign_query_fields = self.format_query(constants.CAMPAIGN_PERFORMANCE_FIELDS)
        campaign_query = f"SELECT {campaign_query_fields} FROM {self.RESOURCE_NAME} WHERE segments.date BETWEEN '{min_date}' AND '{max_date}'"
        campaign_performance = self.ga_service.search(self.account.id, query=campaign_query)

        return campaign_performance, click_type_data

    def _get_campaign_hourly_performance(self):
        """
        Retrieve campaign hourly performance
        :return: Google Ads search response
        """
        min_date = self.today - timedelta(days=10)
        last_entry = self.existing_hourly_statistics.filter(date__lt=min_date).order_by("-date").first()
        if last_entry:
            start_date = last_entry.date
        else:
            start_date = min_date

        # Delete stale data
        self.existing_hourly_statistics.filter(date__gte=start_date).delete()
        hourly_performance_fields = self.format_query(constants.CAMPAIGN_HOURLY_PERFORMANCE_FIELDS)
        hourly_query = f"SELECT {hourly_performance_fields} from {self.RESOURCE_NAME} WHERE segments.date BETWEEN '{start_date}' AND '{self.today}'"
        hourly_performance = self.ga_service.search(self.account.id, query=hourly_query)

        return hourly_performance

    def _create_instances(self, campaign_performance, click_type_data):
        """
        Generator to yield CampaignStatistics instances
        :param campaign_performance: Google ads campaign resource search response
        :return:
        """
        campaign_statistics_to_create = []
        for row in campaign_performance:
            campaign_id = row.campaign.id.value
            budget_type, budget_value = self._get_budget_type_and_value(row)
            budget = float(row.campaign_budget.amount_micros.value if budget_type == BudgetType.DAILY else row.campaign_budget.total_amount_micros.value) / 10 ** 6
            campaign_status, campaign_serving_status = self._get_campaign_statuses(row)
            campaign_data = {
                "de_norm_fields_are_recalculated": False,
                "name": row.campaign.name.value,
                "account": self.account,
                "type": self.channel_type_enum.Name(row.campaign.advertising_channel_type),
                "start_date": row.campaign.start_date.value,
                "end_date": row.campaign.end_date.value,
                "budget": budget,
                "budget_type": budget_type.value,
                "status": campaign_status if campaign_status in ACTION_STATUSES else campaign_serving_status,
                "placement_code": self.extract_placement_code(row.campaign.name.value)
            }
            statistics = {
                "date": row.segments.date.value,
                "campaign_id": row.campaign.id.value,
                "device_id": row.segments.device,
                **self.get_quartile_views(row)
            }
            # Update statistics with click performance obtained in get_clicks_report
            statistics.update(self.get_base_stats(row))
            click_data = self.get_stats_with_click_type_data(statistics, click_type_data, row, resource_name=self.RESOURCE_NAME)
            statistics.update(click_data)
            campaign_statistics_to_create.append(CampaignStatistic(**statistics))
            try:
                campaign = Campaign.objects.get(pk=campaign_id)
                # Continue if the campaign's sync time is less than its update time, as it is pending to be synced with viewiq
                if campaign.sync_time and campaign.sync_time < campaign.update_time:
                    continue
                # Update campaign data
                for field, value in campaign_data.items():
                    setattr(campaign, field, value)
                campaign.save()
            except Campaign.DoesNotExist:
                campaign_data["id"] = str(campaign_id)
                Campaign.objects.create(**campaign_data)

            self.existing_campaigns.add(campaign_id)
        CampaignStatistic.objects.safe_bulk_create(campaign_statistics_to_create)

    def _create_hourly_instances(self, hourly_performance):
        """
        Create CampaignHourlyStatistic objects
        :param hourly_performance: :param campaign_performance: Google ads campaign resource search response segmented by hour
        :return:
        """
        campaigns_to_create = []
        hourly_statistics_to_create = []
        self.existing_campaigns = set(Campaign.objects.all().values_list("id", flat=True))
        for row in hourly_performance:
            campaign_id = str(row.campaign.id.value)
            campaign_status, campaign_serving_status = self._get_campaign_statuses(row)
            if campaign_id not in self.existing_campaigns:
                campaign = Campaign(
                    id=campaign_id,
                    name=row.campaign.name.value,
                    account=self.account,
                    type=self.channel_type_enum.Name(row.campaign.advertising_channel_type),
                    start_date=row.campaign.start_date.value,
                    end_date=row.campaign.end_date.value,
                    budget=float(row.campaign_budget.amount_micros.value) / 10 ** 6,
                    status=campaign_status if campaign_status in ACTION_STATUSES else campaign_serving_status,
                    impressions=1,
                    # to show this item on the accounts lists Track/Filters
                )
                campaigns_to_create.append(campaign)
                self.existing_campaigns.add(campaign_id)

            hourly_stat = CampaignHourlyStatistic(
                date=row.segments.date.value,
                hour=row.segments.hour.value,
                campaign_id=campaign_id,
                video_views=row.metrics.video_views.value,
                impressions=row.metrics.impressions.value,
                clicks=row.metrics.clicks.value,
                cost=float(row.metrics.cost_micros.value) / 10**6
            )
            hourly_statistics_to_create.append(hourly_stat)
        Campaign.objects.bulk_create(campaigns_to_create)
        CampaignHourlyStatistic.objects.bulk_create(hourly_statistics_to_create)

    def _get_budget_type_and_value(self, row):
        budget_type = BudgetType.DAILY if row.campaign_budget.amount_micros.value is not None else BudgetType.TOTAL
        budget_value = float(row.campaign_budget.amount_micros.value if budget_type == BudgetType.DAILY else row.campaign_budget.total_amount_micros.value) / 10 ** 6
        return budget_type, budget_value

    def _get_campaign_statuses(self, row):
        campaign_status = self.campaign_status_enum.Name(row.campaign.status).lower()
        campaign_serving_status = self.campaign_serving_status_enum.Name(row.campaign.serving_status).lower()
        return campaign_status, campaign_serving_status
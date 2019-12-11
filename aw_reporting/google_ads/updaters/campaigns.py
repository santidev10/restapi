from datetime import datetime
from datetime import timedelta

from django.db.models import Max
from django.db import transaction
from django.utils import timezone
import logging
import pytz

from aw_reporting.adwords_reports import campaign_performance_report
from aw_reporting.adwords_reports import MAIN_STATISTICS_FILEDS
from aw_reporting.google_ads import constants
from aw_reporting.google_ads.update_mixin import UpdateMixin
from aw_reporting.models import Account
from aw_reporting.models import ACTION_STATUSES
from aw_reporting.models import Campaign
from aw_reporting.models import CampaignHourlyStatistic
from aw_reporting.models import CampaignStatistic
from aw_reporting.models.ad_words.constants import BudgetType
from aw_reporting.models.ad_words.constants import get_device_id_by_name
from aw_reporting.update.adwords_utils import format_click_types_report
from aw_reporting.update.adwords_utils import get_base_stats
from aw_reporting.update.adwords_utils import quart_views
from aw_reporting.update.adwords_utils import update_stats_with_click_type_data
from utils.datetime import now_in_default_tz


logger = logging.getLogger(__name__)


TRACKING_CLICK_TYPES = (
    ("Website", "clicks_website"),
    ("Call-to-Action overlay", "clicks_call_to_action_overlay"),
    ("App store", "clicks_app_store"),
    ("Cards", "clicks_cards"),
    ("End cap", "clicks_end_cap")
)


class CampaignUpdater(UpdateMixin):
    RESOURCE_NAME = "campaign"

    def __init__(self, account):
        self.account = account
        self.today = datetime.now(tz=pytz.timezone(account.timezone)).date()
        self.existing_campaigns = set()
        self.client = None

    def update(self, client, *args, **kwargs):
        self.client = client
        self.update_campaigns()
        self.update_hourly_campaigns()
        # Update account
        Account.objects.filter(id=self.account.id).update(hourly_updated_at=timezone.now())

    def update_campaigns(self):
        now = now_in_default_tz()
        today = now.date()
        max_date = self.max_ready_date(now, tz_str=self.account.timezone)

        stats_queryset = CampaignStatistic.objects.filter(
            campaign__account=self.account
        )
        self.drop_latest_stats(stats_queryset, today)

        # lets find min and max dates for the report request
        dates = stats_queryset.aggregate(max_date=Max("date"))
        min_date = dates["max_date"] + timedelta(days=1) \
            if dates["max_date"] \
            else constants.MIN_FETCH_DATE
        report = campaign_performance_report(
            self.client,
            dates=(min_date, max_date),
            include_zero_impressions=False,
            additional_fields=("Device", "Date"),
        )
        click_type_fields = (
            "CampaignId",
            "Date",
            "Clicks",
            "ClickType",
        )
        click_type_report = campaign_performance_report(
            self.client, dates=(min_date, max_date), fields=click_type_fields, include_zero_impressions=False)
        click_type_data = format_click_types_report(click_type_report, "CampaignId", "CampaignId")
        insert_stat = []
        for row_obj in report:
            campaign_id = row_obj.CampaignId
            try:
                end_date = datetime.strptime(row_obj.EndDate, constants.GET_DF)
            except ValueError:
                end_date = None

            if row_obj.CampaignStatus in ACTION_STATUSES:
                status = row_obj.CampaignStatus
            else:
                status = "serving" if row_obj.ServingStatus == "eligible" else row_obj.ServingStatus

            name = row_obj.CampaignName
            placement_code = self.extract_placement_code(name)
            budget_type = BudgetType.DAILY if row_obj.TotalAmount.strip() == "--" else BudgetType.TOTAL
            budget_str = row_obj.Amount if budget_type == BudgetType.DAILY else row_obj.TotalAmount
            budget = float(budget_str) / 1000000
            stats = {
                "de_norm_fields_are_recalculated": False,
                "name": name,
                "account": self.account,
                "type": row_obj.AdvertisingChannelType,
                "start_date": datetime.strptime(row_obj.StartDate, constants.GET_DF),
                "end_date": end_date,
                "budget": budget,
                "budget_type": budget_type.value,
                "status": status,
                "placement_code": placement_code
            }

            statistic_data = {
                "date": row_obj.Date,
                "campaign_id": row_obj.CampaignId,
                "device_id": get_device_id_by_name(row_obj.Device),

                "video_views_25_quartile": quart_views(row_obj, 25),
                "video_views_50_quartile": quart_views(row_obj, 50),
                "video_views_75_quartile": quart_views(row_obj, 75),
                "video_views_100_quartile": quart_views(row_obj, 100),
            }
            statistic_data.update(get_base_stats(row_obj))
            update_stats_with_click_type_data(
                statistic_data, click_type_data, row_obj, unique_field_name="CampaignId", ref_id_name="CampaignId")

            insert_stat.append(CampaignStatistic(**statistic_data))

            try:
                campaign = Campaign.objects.get(pk=campaign_id)

                # Continue if the campaign's sync time is less than its update time, as it is pending to be synced with viewiq
                if campaign.sync_time and campaign.sync_time < campaign.update_time:
                    continue

                for field, value in stats.items():
                    setattr(campaign, field, value)
                campaign.save()
            except Campaign.DoesNotExist:
                stats["id"] = campaign_id
                Campaign.objects.create(**stats)

        if insert_stat:
            CampaignStatistic.objects.safe_bulk_create(insert_stat)

    def update_hourly_campaigns(self):
        statistic_queryset = CampaignHourlyStatistic.objects.filter(
            campaign__account=self.account)

        today = datetime.now(tz=pytz.timezone(self.account.timezone)).date()
        min_date = today - timedelta(days=10)

        last_entry = statistic_queryset.filter(date__lt=min_date) \
            .order_by("-date").first()

        start_date = min_date
        if last_entry:
            start_date = last_entry.date

        statistic_to_drop = statistic_queryset.filter(date__gte=start_date)

        report = campaign_performance_report(
            self.client,
            dates=(start_date, today),
            fields=["CampaignId", "CampaignName", "StartDate", "EndDate",
                    "AdvertisingChannelType", "Amount", "CampaignStatus",
                    "ServingStatus", "Date", "HourOfDay"
                    ] + list(MAIN_STATISTICS_FILEDS[:4]),
            include_zero_impressions=False)

        if not report:
            return

        campaign_ids = list(
            self.account.campaigns.values_list("id", flat=True)
        )
        create_campaign = []
        create_stat = []
        for row in report:
            campaign_id = row.CampaignId
            if campaign_id not in campaign_ids:
                campaign_ids.append(campaign_id)
                try:
                    end_date = datetime.strptime(row.EndDate, constants.GET_DF)
                except ValueError:
                    end_date = None
                create_campaign.append(
                    Campaign(
                        id=campaign_id,
                        name=row.CampaignName,
                        account=self.account,
                        type=row.AdvertisingChannelType,
                        start_date=datetime.strptime(row.StartDate, constants.GET_DF),
                        end_date=end_date,
                        budget=float(row.Amount) / 1000000,
                        status=row.CampaignStatus if row.CampaignStatus in ACTION_STATUSES else row.ServingStatus,
                        impressions=1,
                        # to show this item on the accounts lists Track/Filters
                    )
                )

            create_stat.append(
                CampaignHourlyStatistic(
                    date=row.Date,
                    hour=row.HourOfDay,
                    campaign_id=row.CampaignId,
                    video_views=row.VideoViews,
                    impressions=row.Impressions,
                    clicks=row.Clicks,
                    cost=float(row.Cost) / 1000000,
                )
            )

        with transaction.atomic():
            if create_campaign:
                Campaign.objects.bulk_create(create_campaign)

            statistic_to_drop.delete()

            if create_stat:
                CampaignHourlyStatistic.objects.bulk_create(create_stat)

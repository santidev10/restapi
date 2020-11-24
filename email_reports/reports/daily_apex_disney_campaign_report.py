import logging

from django.conf import settings
from django.contrib.auth import get_user_model
from django.db.models import Max

from aw_reporting.models import AdStatistic
from email_reports.reports.daily_apex_visa_campaign_report import DailyApexVisaCampaignEmailReport
from email_reports.reports.daily_apex_visa_campaign_report import DATE_FORMAT

logger = logging.getLogger(__name__)


class DailyApexDisneyCampaignEmailReport(DailyApexVisaCampaignEmailReport):

    CAMPAIGNS_FIELDS = ("account__id", "salesforce_placement__opportunity__ias_campaign_name",
                        "salesforce_placement__opportunity__disney_campaign_advertiser_id",
                        "salesforce_placement__disney_datorama_placement_name",
                        "salesforce_placement__apex_go_client_rate", "id", "name",
                        "salesforce_placement__goal_type_id", "salesforce_placement__ordered_rate",)
    STATS_FIELDS = ("date", "impressions", "clicks", "video_views", "cost", "video_views_25_quartile",
                    "video_views_50_quartile", "video_views_75_quartile", "video_views_100_quartile")
    CSV_HEADER = ("Campaign Advertiser ID", "Campaign Advertiser", "Campaign ID", "Campaign Name", "Placement ID",
                  "Placement Name", "Creative ID", "Creative Name", "Date", "Currency", "Media Cost", "Impressions",
                  "Clicks", "Video Views", "Video Views (25%)", "Video Views (50%)", "Video Views (75%)",
                  "Video Completions",)
    attachment_filename = "daily_campaign_report.csv"
    historical_filename = "apex_disney_historical.csv"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.user = get_user_model().objects.filter(email=settings.DAILY_APEX_DISNEY_CAMPAIGN_REPORT_CREATOR).first()
        self.to = settings.DAILY_APEX_DISNEY_REPORT_TO_EMAILS

    def _get_subject(self):
        return f"Daily Disney Campaign Report for {self.yesterday}"

    def get_stats(self, campaign_ids):
        """
        get stats day-by-day, instead of a summed "running total". If
        is_historical is set, then results are not constrained to only
        yesterday's.
        """
        filter_kwargs = {"ad__ad_group__campaign__id__in": campaign_ids, }
        if not self.is_historical:
            filter_kwargs["date"] = self.yesterday

        return AdStatistic.objects.values(
                "ad__id",
                "ad__creative_name",
                "ad__ad_group__campaign__id",
                "ad__ad_group__campaign__account_id",
                "ad__ad_group__id",
                "ad__ad_group__name",
                # stats fields
                "impressions",
                "clicks",
                "video_views",
                "cost",
                "video_views_25_quartile",
                "video_views_50_quartile",
                "video_views_75_quartile",
                "video_views_100_quartile",
        ) \
            .filter(**filter_kwargs) \
            .annotate(
                ad__ad_group__videos_stats__creative__id=Max("ad__ad_group__videos_stats__creative__id"),
                ad__ad_group__campaign__salesforce_placement__goal_type_id=Max(
                  "ad__ad_group__campaign__salesforce_placement__goal_type_id"
                ),
                ad__ad_group__campaign__salesforce_placement__ordered_rate=Max(
                    "ad__ad_group__campaign__salesforce_placement__ordered_rate"
                ),
                ad__ad_group__campaign__account__name=Max("ad__ad_group__campaign__account__name"),
                ad__ad_group__campaign__salesforce_placement__opportunity__disney_campaign_advertiser_id=Max(
                    "ad__ad_group__campaign__salesforce_placement__opportunity__disney_campaign_advertiser_id"
                ),
                ad__ad_group__campaign__salesforce_placement__disney_datorama_placement_name=Max(
                    "ad__ad_group__campaign__salesforce_placement__disney_datorama_placement_name"
                ),
                ad__ad_group__campaign__salesforce_placement__apex_go_client_rate=Max(
                    "ad__ad_group__campaign__salesforce_placement__apex_go_client_rate"
                ),
            ) \
            .order_by("date") \
            .values_list(
                *[f"ad__ad_group__campaign__{field}" for field in self.CAMPAIGNS_FIELDS] + list(self.STATS_FIELDS),
                "ad__ad_group__id",
                "ad__ad_group__name",
                "ad__id",
                "ad__creative_name",
                "ad__ad_group__videos_stats__creative__id",
                named=True
            )

    def get_rows_from_stats(self, creative_statistics):
        rows = []
        creative_statistics = list(creative_statistics)

        for stats in creative_statistics:
            # Only show line items that are attached to salesforce placements
            if not stats.ad__ad_group__campaign__salesforce_placement__goal_type_id:
                continue

            ias_campaign_name = stats.ad__ad_group__campaign__salesforce_placement__opportunity__ias_campaign_name
            media_cost = self._get_revenue(
                stats,
                "ad__ad_group__campaign__",
                "ad__ad_group__campaign__salesforce_placement__apex_go_client_rate")
            rows.append([
                stats.ad__ad_group__campaign__account__id,
                stats.ad__ad_group__campaign__salesforce_placement__opportunity__disney_campaign_advertiser_id,
                stats.ad__ad_group__campaign__id,
                ias_campaign_name or self.get_campaign_name(stats.ad__ad_group__campaign__account__name),
                stats.ad__ad_group__id,
                stats.ad__ad_group__campaign__salesforce_placement__disney_datorama_placement_name,
                stats.ad__ad_group__videos_stats__creative__id,
                stats.ad__creative_name,
                stats.date.strftime(DATE_FORMAT),
                "GBP",
                media_cost,
                stats.impressions,
                stats.clicks,
                stats.video_views,
                int(stats.video_views_25_quartile),
                int(stats.video_views_50_quartile),
                int(stats.video_views_75_quartile),
                int(stats.video_views_100_quartile),
            ])
        return rows

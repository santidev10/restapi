import logging
from datetime import timedelta

from django.conf import settings
from django.contrib.auth import get_user_model
from django.db.models import Max
from django.db.models import Sum

from aw_reporting.models import VideoCreativeStatistic
from email_reports.reports import DailyApexCampaignEmailReport
from email_reports.reports.daily_apex_campaign_report import DATE_FORMAT
from es_components.constants import Sections

logger = logging.getLogger(__name__)


class DailyApexDisneyCampaignEmailReport(DailyApexCampaignEmailReport):

    CAMPAIGNS_FIELDS = ("account__id", "salesforce_placement__opportunity__ias_campaign_name", "id", "name",
                        "salesforce_placement__goal_type_id",)
    STATS_FIELDS = ("date", "impressions", "clicks", "video_views", "cost", "video_views_25_quartile",
                    "video_views_50_quartile", "video_views_75_quartile", "video_views_100_quartile", "ad_group")
    CSV_HEADER = ("Campaign Advertiser ID", "Campaign Advertiser", "Campaign ID", "Campaign Name", "Placement ID",
                  "Placement Name", "Creative ID", "Creative Name", "Date", "Currency", "Media Cost", "Impressions",
                  "Clicks", "Video Views", "Video Views (25%)", "Video Views (50%)", "Video Views (75%)",
                  "Video Completions",)
    from_email = settings.EXPORTS_EMAIL_ADDRESS
    to = settings.DAILY_APEX_DISNEY_REPORT_TO_EMAILS
    cc = settings.DAILY_APEX_REPORT_CC_EMAIL_ADDRESSES
    attachment_filename = "report.csv"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.user = get_user_model().objects.filter(email=settings.DAILY_APEX_DISNEY_CAMPAIGN_REPORT_CREATOR).first()
        self.yesterday = self.today - timedelta(days=1)

    def _get_subject(self):
        if self.is_historical:
            return f"Historical Disney Campaign Report for {self.today}"
        return f"Daily Disney Campaign Report for {self.yesterday}"

    def get_stats(self, campaign_ids):
        """
        get stats day-by-day, instead of a summed "running total". If
        is_historical is set, then results are not constrained to only
        yesterday's.
        """
        filter_kwargs = {"ad_group__campaign__id__in": campaign_ids, }
        if not self.is_historical:
            filter_kwargs["date"] = self.yesterday

        return VideoCreativeStatistic.objects.values(
                "ad_group__campaign__id",
                "ad_group__campaign__account_id",
                "creative_id",
                "ad_group__id",
                "ad_group__name"
            ) \
            .filter(**filter_kwargs) \
            .annotate(
                impressions=Sum("impressions"),
                clicks=Sum("clicks"),
                video_views=Sum("video_views"),
                cost=Sum("cost"),
                video_views_25_quartile=Sum("video_views_25_quartile"),
                video_views_50_quartile=Sum("video_views_50_quartile"),
                video_views_75_quartile=Sum("video_views_75_quartile"),
                video_views_100_quartile=Sum("video_views_100_quartile"),
                ad_group__campaign__salesforce_placement__goal_type_id=Max(
                  "ad_group__campaign__salesforce_placement__goal_type_id"
                ),
                ad_group__campaign__account__name=Max("ad_group__campaign__account__name"),
                ad_group__campaign__account__currency_code=Max("ad_group__campaign__account__currency_code")
            ) \
            .order_by("date") \
            .values_list(*[f"ad_group__campaign__{field}" for field in self.CAMPAIGNS_FIELDS] + list(self.STATS_FIELDS),
                         "creative_id", "ad_group__id", "ad_group__name", named=True)

    def get_rows_from_stats(self, creative_statistics):
        rows = []
        creative_statistics = list(creative_statistics)

        creatives_info = self._get_creative_info([stats.creative_id for stats in creative_statistics])

        for stats in creative_statistics:
            ias_campaign_name = stats.ad_group__campaign__salesforce_placement__opportunity__ias_campaign_name
            rows.append([
                stats.ad_group__campaign__account__id,
                ias_campaign_name,
                stats.ad_group__campaign__id,
                stats.ad_group__campaign__name,
                stats.ad_group__id,
                stats.ad_group__name,
                stats.creative_id,
                creatives_info.get(stats.creative_id, {}).get(Sections.GENERAL_DATA, {}).get("title"),
                stats.date.strftime(DATE_FORMAT),
                "GBP",
                stats.cost,
                stats.impressions,
                stats.clicks,
                stats.video_views,
                int(stats.video_views_25_quartile),
                int(stats.video_views_50_quartile),
                int(stats.video_views_75_quartile),
                int(stats.video_views_100_quartile),
            ])
        return rows

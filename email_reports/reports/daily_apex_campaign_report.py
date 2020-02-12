import csv
import logging

from io import StringIO
from datetime import timedelta

from django.conf import settings
from django.core.mail import EmailMessage
from django.contrib.auth import get_user_model

from aw_reporting.models import device_str
from aw_reporting.models import Campaign
from aw_reporting.models import CampaignStatistic
from aw_reporting.models import CampaignStatus
from aw_reporting.models import SalesForceGoalType
from aw_reporting.models import VideoCreativeStatistic
from email_reports.reports.base import BaseEmailReport
from utils.datetime import now_in_default_tz

logger = logging.getLogger(__name__)


CAMPAIGNS_FIELDS = ("name", "id", "account_id", "account__currency_code", "account__name",
                    "salesforce_placement__ordered_rate", "salesforce_placement__goal_type_id")

STATS_FIELDS = ("date", "impressions", "clicks", "video_views_100_quartile", "video_views_50_quartile",
                "video_views")

CSV_HEADER = ("Date", "CID Name/Number", "Advertiser Currency", "Device Type", "Campaign ID", "Campaign",
              "Creative ID", "Revenue (Adv Currency)", "Impressions", "Clicks", "TrueView: Views",
              "Midpoint Views (Video)", "Complete Views (Video)")

DATE_FORMAT = "%d/%m/%y"


class DailyApexCampaignEmailReport(BaseEmailReport):
    def __init__(self, *args, **kwargs):
        # added just for testing on RC. Should be removed after testing
        # kwargs["debug"] = False

        super(DailyApexCampaignEmailReport, self).__init__(*args, **kwargs)

        self.today = now_in_default_tz().date()
        self.yesterday = self.today - timedelta(days=1)

    def send(self):
        user = get_user_model().objects.filter(email=settings.DAILY_APEX_CAMPAIGN_REPORT_CREATOR)

        if not user.exists():
            return

        csv_context = self._get_csv_file_context(user.first())

        msg = EmailMessage(
                subject=self._get_subject(),
                body=self._get_body(),
                from_email=settings.EXPORTS_EMAIL_ADDRESS,
                to=self.get_to(settings.DAILY_APEX_REPORT_EMAIL_ADDRESSES),
                cc=self.get_cc(settings.CF_AD_OPS_DIRECTORS),
                bcc=self.get_bcc(),
        )

        msg.attach('daily_campaign_report.csv', csv_context, 'text/csv')
        msg.send(fail_silently=False)

    def _get_subject(self):
        return f"Daily Campaign Report for {self.yesterday}"

    def _get_body(self):
        return f"Daily Campaign Report for {self.yesterday}. \nPlease see attached file."

    def _get_csv_file_context(self, user):
        campaigns = Campaign.objects.get_queryset_for_user(user=user).values_list("id", flat=True)
        campaigns_ids = list(campaigns)

        campaigns_statistics = self.__get_campaign_statistics(campaigns_ids)
        video_creative_statistics = self.__get_video_creative_statistics(campaigns_ids)

        if not campaigns_statistics.exists() and not video_creative_statistics.exists():
            return None

        csv_file = StringIO()
        writer = csv.writer(csv_file)
        writer.writerow(CSV_HEADER)

        writer.writerows(self.__get_campaign_statistics_rows(campaigns_statistics))
        writer.writerows(self.__get_creative_statistics_rows(video_creative_statistics))

        return csv_file.getvalue()


    def __get_revenue(self, obj, campaign_prefix):
        goal_type_id = getattr(obj, f"{campaign_prefix}salesforce_placement__goal_type_id")
        ordered_rate = getattr(obj, f"{campaign_prefix}salesforce_placement__ordered_rate")

        if goal_type_id == SalesForceGoalType.CPV:
            return round(ordered_rate * obj.video_views, 2)
        elif goal_type_id == SalesForceGoalType.CPM:
            return round(ordered_rate * obj.impressions / 1000, 2)


    def __get_campaign_statistics(self, campaign_ids):
        return CampaignStatistic.objects\
            .filter(campaign_id__in=campaign_ids, date=self.yesterday) \
            .values_list(*[f"campaign__{field}" for field in CAMPAIGNS_FIELDS] + list(STATS_FIELDS), "device_id",
                         named=True)


    def __get_campaign_statistics_rows(self, campaigns_statistics):
        rows = []

        for stats in campaigns_statistics:
            rows.append([
                stats.date.strftime(DATE_FORMAT),
                stats.campaign__account__name or stats.campaign__account_id,
                stats.campaign__account__currency_code,
                device_str(stats.device_id),
                stats.campaign__id,
                stats.campaign__name,
                None,
                self.__get_revenue(stats, "campaign__"),
                *self._get_stats_metrics(stats)
            ])
        return rows


    def __get_video_creative_statistics(self, campaign_ids):
        return VideoCreativeStatistic.objects\
            .filter(ad_group__campaign_id__in=campaign_ids, date=self.yesterday)\
            .values_list(*[f"ad_group__campaign__{field}" for field in CAMPAIGNS_FIELDS]
                    + list(STATS_FIELDS), "creative_id", named=True)

    def __get_creative_statistics_rows(self, creative_statistics):
        rows = []

        for stats in creative_statistics:
            rows.append([
                stats.date.strftime(DATE_FORMAT),
                stats.ad_group__campaign__account__name or stats.ad_group__campaign__account_id,
                stats.ad_group__campaign__account__currency_code,
                None,
                stats.ad_group__campaign__id,
                stats.ad_group__campaign__name,
                stats.creative_id,
                self.__get_revenue(stats, "ad_group__campaign__"),
                *self._get_stats_metrics(stats)
            ])
        return rows

    def _get_stats_metrics(self, stats):
        return (
            stats.impressions,
            stats.clicks,
            stats.video_views,
            int(stats.video_views_50_quartile),
            int(stats.video_views_100_quartile),
        )

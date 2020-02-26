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
from aw_reporting.models import SalesForceGoalType
from aw_reporting.models import VideoCreativeStatistic
from es_components.constants import Sections
from es_components.managers import VideoManager
from email_reports.reports.base import BaseEmailReport
from utils.datetime import now_in_default_tz
from userprofile.constants import UserSettingsKey
from utils.youtube_api import resolve_videos_info

logger = logging.getLogger(__name__)


CAMPAIGNS_FIELDS = ("name", "id", "account_id", "account__currency_code", "account__name",
                    "salesforce_placement__ordered_rate", "salesforce_placement__goal_type_id")

STATS_FIELDS = ("date", "impressions", "clicks", "video_views_100_quartile", "video_views_50_quartile",
                "video_views")

CSV_HEADER = ("Date", "CID Name/Number", "Advertiser Currency", "Device Type", "Campaign ID", "Campaign", "Creative ID",
              "Creative", "Creative Source", "Revenue (Adv Currency)", "Impressions", "Clicks", "TrueView: Views",
              "Midpoint Views (Video)", "Complete Views (Video)")

DATE_FORMAT = "%m/%d/%y"
YOUTUBE_LINK_TEMPLATE = "https://www.youtube.com/watch?v={}"


class DailyApexCampaignEmailReport(BaseEmailReport):
    def __init__(self, *args, **kwargs):
        super(DailyApexCampaignEmailReport, self).__init__(*args, **kwargs)

        self.today = now_in_default_tz().date()
        self.yesterday = self.today - timedelta(days=1)

    def send(self):
        user = get_user_model().objects.filter(email=settings.DAILY_APEX_CAMPAIGN_REPORT_CREATOR)

        if not user.exists():
            return

        csv_context = self._get_csv_file_context(user.first())

        if not csv_context:
            logger.error("No data to send apex daily campaign report.")
            return

        msg = EmailMessage(
                subject=self._get_subject(),
                body=self._get_body(),
                from_email=settings.EXPORTS_EMAIL_ADDRESS,
                to=self.get_to(settings.DAILY_APEX_REPORT_EMAIL_ADDRESSES),
                cc=self.get_cc([]),
                bcc=self.get_bcc(),
        )

        msg.attach('daily_campaign_report.csv', csv_context, 'text/csv')
        msg.send(fail_silently=False)

    def _get_subject(self):
        return f"Daily Campaign Report for {self.yesterday}"

    def _get_body(self):
        return f"Daily Campaign Report for {self.yesterday}. \nPlease see attached file."

    def _get_csv_file_context(self, user):
        campaigns = Campaign.objects.filter(account_id__in=user.aw_settings.get(UserSettingsKey.VISIBLE_ACCOUNTS))\
            .values_list("id", flat=True)
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
                None,
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
        creative_statistics = list(creative_statistics)

        creatives_info = self.__get_creative_info([stats.creative_id for stats in creative_statistics])

        for stats in creative_statistics:
            rows.append([
                stats.date.strftime(DATE_FORMAT),
                stats.ad_group__campaign__account__name or stats.ad_group__campaign__account_id,
                stats.ad_group__campaign__account__currency_code,
                None,
                stats.ad_group__campaign__id,
                stats.ad_group__campaign__name,
                stats.creative_id,
                creatives_info.get(stats.creative_id, {}).get(Sections.GENERAL_DATA, {}).get("title"),
                YOUTUBE_LINK_TEMPLATE.format(stats.creative_id),
                self.__get_revenue(stats, "ad_group__campaign__"),
                *self._get_stats_metrics(stats)
            ])
        return rows


    def __get_creative_info(self, creative_ids):
        manager = VideoManager(Sections.GENERAL_DATA)
        videos_map = {}
        for video in manager.get(ids=creative_ids, skip_none=True):
            videos_map[video.main.id] = video.to_dict()

        unresolved_ids = list(set(creative_ids) - set(videos_map.keys()))
        unresolved_videos_info = resolve_videos_info(unresolved_ids) if unresolved_ids else {}

        return {**videos_map, **unresolved_videos_info}

    def _get_stats_metrics(self, stats):
        return (
            stats.impressions,
            stats.clicks,
            stats.video_views,
            int(stats.video_views_50_quartile),
            int(stats.video_views_100_quartile),
        )

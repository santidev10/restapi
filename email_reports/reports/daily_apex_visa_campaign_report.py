import csv
import logging
import os
from datetime import timedelta
from io import StringIO
from typing import Union

from django.conf import settings
from django.contrib.auth import get_user_model
from django.core.mail import EmailMessage
from django.db.models import Max
from django.db.models import Sum

from aw_reporting.models import Campaign
from aw_reporting.models import SalesForceGoalType
from aw_reporting.models import VideoCreativeStatistic
from email_reports.reports.base import BaseEmailReport
from es_components.constants import Sections
from es_components.managers import VideoManager
from userprofile.constants import UserSettingsKey
from utils.datetime import now_in_default_tz
from utils.youtube_api import resolve_videos_info

logger = logging.getLogger(__name__)

DATE_FORMAT = "%m/%d/%y"
YOUTUBE_LINK_TEMPLATE = "https://www.youtube.com/watch?v={}"


class DailyApexVisaCampaignEmailReport(BaseEmailReport):

    CAMPAIGNS_FIELDS = ("id", "account__name", "account__currency_code", "salesforce_placement__ordered_rate",
                        "salesforce_placement__goal_type_id", "salesforce_placement__opportunity__ias_campaign_name")
    STATS_FIELDS = ("date", "impressions", "clicks", "video_views_100_quartile", "video_views_50_quartile",
                    "video_views")
    CSV_HEADER = ("Date", "Advertiser Currency", "Device Type", "Campaign ID", "Campaign", "Creative ID",
                  "Creative", "Creative Source", "Revenue (Adv Currency)", "Impressions", "Clicks", "TrueView: Views",
                  "Midpoint Views (Video)", "Complete Views (Video)")
    attachment_filename = "daily_campaign_report.csv"
    historical_filename = "apex_visa_historical.csv"

    def __init__(self, *args, **kwargs):
        """
        is_historical: Bool: If True, fetches ALL VideoCreativeStatistic records
        for the given Accounts ids, rather than just the previous day's
        """
        super().__init__(*args, **kwargs)

        self.today = now_in_default_tz().date()
        self.yesterday = self.today - timedelta(days=1)
        self.user = get_user_model().objects.filter(email=settings.DAILY_APEX_CAMPAIGN_REPORT_CREATOR).first()
        self.from_email = settings.EXPORTS_EMAIL_ADDRESS
        self.to = settings.DAILY_APEX_REPORT_EMAIL_ADDRESSES
        self.cc = settings.DAILY_APEX_REPORT_CC_EMAIL_ADDRESSES

    def historical(self):
        """
        write a historical report to the local filesystem. DOES NOT SEND email report
        :return:
        """
        self.is_historical = True
        self._write_historical()

    def send(self):
        """
        used by automated task to send the daily report to defined recipients
        :return:
        """
        if not self.to:
            logger.error(f"No recipients set for {self.__class__.__name__} Apex campaign report")
            return

        if not isinstance(self.user, get_user_model()):
            return

        csv_context = self._get_csv_file_context()
        if not csv_context:
            logger.error(f"No data to send {self.__class__.__name__} Apex campaign report.")
            return

        msg = EmailMessage(
            subject=self._get_subject(),
            body=self._get_body(),
            from_email=self.from_email,
            to=self.get_to(self.to),
            cc=self.get_cc(self.cc),
            bcc=self.get_bcc()
        )

        msg.attach(self.attachment_filename, csv_context, "text/csv")
        msg.send(fail_silently=False)

    def _get_subject(self):
        return f"Daily Campaign Report for {self.yesterday}"

    def _get_body(self):
        return f"Daily Campaign Report for {self.yesterday}. \nPlease see attached file."

    def get_account_ids(self):
        return self.user.aw_settings.get(UserSettingsKey.VISIBLE_ACCOUNTS)

    def _get_campaign_ids(self):
        account_ids = self.get_account_ids()
        campaigns = Campaign.objects.filter(account_id__in=account_ids) \
            .values_list("id", flat=True)
        campaigns_ids = list(campaigns)
        return campaigns_ids

    def _write_historical(self):
        """
        write historical csv data locally to self.historical_filename
        :return:
        """
        campaign_ids = self._get_campaign_ids()
        stats = self.get_stats(campaign_ids)

        if not stats.exists():
            return

        try:
            os.remove(self.historical_filename)
        except FileNotFoundError:
            pass
        with open(self.historical_filename, mode="w") as f:
            writer = csv.writer(f)
            writer.writerow(self.CSV_HEADER)
            writer.writerows(self.get_rows_from_stats(stats))
        print(f"wrote historical data to filename: {self.historical_filename}")

    def _get_csv_file_context(self):
        """
        get csv data to be sent in daily email
        :return:
        """
        campaign_ids = self._get_campaign_ids()
        stats = self.get_stats(campaign_ids)

        if not stats.exists():
            return None

        csv_file = StringIO()
        writer = csv.writer(csv_file)
        writer.writerow(self.CSV_HEADER)
        writer.writerows(self.get_rows_from_stats(stats))
        return csv_file.getvalue()

    @staticmethod
    def _get_revenue(obj, campaign_prefix, rate_field=None) -> Union[float, None]:
        """
        calcuate revenue from the given stats object. If not calculable, return None
        :param obj:
        :param campaign_prefix:
        :param rate_field:
        :return: float/none
        """
        goal_type_id = getattr(obj, f"{campaign_prefix}salesforce_placement__goal_type_id")
        ordered_rate = getattr(obj, rate_field) if rate_field \
            else getattr(obj, f"{campaign_prefix}salesforce_placement__ordered_rate")

        # validate
        if ordered_rate is None or goal_type_id is None:
            return None
        try:
            ordered_rate = float(ordered_rate)
        except ValueError:
            return None

        if goal_type_id == SalesForceGoalType.CPV:
            return round(ordered_rate * obj.video_views, 2)
        if goal_type_id == SalesForceGoalType.CPM:
            return round(ordered_rate * obj.impressions / 1000, 2)
        return None

    @staticmethod
    def get_campaign_name(account_name):
        return settings.APEX_CAMPAIGN_NAME_SUBSTITUTIONS.get(account_name, None)

    def get_stats(self, campaign_ids):
        """
        get stats day-by-day, instead of a summed "running total". If
        is_historical is set, then results are not constrained to only
        yesterday's.
        """
        filter_kwargs = {"ad_group__campaign__id__in": campaign_ids, }
        if not self.is_historical:
            filter_kwargs["date"] = self.yesterday

        return VideoCreativeStatistic.objects.values("ad_group__campaign__id", "creative_id") \
            .filter(**filter_kwargs) \
            .annotate(impressions=Sum("impressions"),
                      clicks=Sum("clicks"),
                      video_views=Sum("video_views"),
                      video_views_50_quartile=Sum("video_views_50_quartile"),
                      video_views_100_quartile=Sum("video_views_100_quartile"),
                      ad_group__campaign__salesforce_placement__goal_type_id=Max(
                          "ad_group__campaign__salesforce_placement__goal_type_id"
                      ),
                      ad_group__campaign__salesforce_placement__ordered_rate=Max(
                          "ad_group__campaign__salesforce_placement__ordered_rate"
                      ),
                      ad_group__campaign__account__name=Max("ad_group__campaign__account__name"),
                      ad_group__campaign__account__currency_code=Max("ad_group__campaign__account__currency_code")
                      ) \
            .order_by("date") \
            .values_list(*[f"ad_group__campaign__{field}" for field in self.CAMPAIGNS_FIELDS] + list(self.STATS_FIELDS),
                         "creative_id", named=True)

    def get_rows_from_stats(self, creative_statistics):
        rows = []
        creative_statistics = list(creative_statistics)

        creatives_info = self._get_creative_info([stats.creative_id for stats in creative_statistics])

        for stats in creative_statistics:
            ias_campaign_name = stats.ad_group__campaign__salesforce_placement__opportunity__ias_campaign_name
            rows.append([
                stats.date.strftime(DATE_FORMAT),
                "EUR",  # stats.ad_group__campaign__account__currency_code,
                "Cross Device",
                stats.ad_group__campaign__id,
                ias_campaign_name or self.get_campaign_name(stats.ad_group__campaign__account__name),
                stats.creative_id,
                creatives_info.get(stats.creative_id, {}).get(Sections.GENERAL_DATA, {}).get("title"),
                YOUTUBE_LINK_TEMPLATE.format(stats.creative_id),
                self._get_revenue(stats, "ad_group__campaign__"),
                *self._get_stats_metrics(stats)
            ])
        return rows

    @staticmethod
    def _get_creative_info(creative_ids):
        manager = VideoManager(Sections.GENERAL_DATA)
        videos_map = {}
        for video in manager.get(ids=creative_ids, skip_none=True):
            videos_map[video.main.id] = video.to_dict()

        unresolved_ids = list(set(creative_ids) - set(videos_map.keys()))
        unresolved_videos_info = resolve_videos_info(unresolved_ids) if unresolved_ids else {}

        return {**videos_map, **unresolved_videos_info}

    @staticmethod
    def _get_stats_metrics(stats):
        return (
            stats.impressions,
            stats.clicks,
            stats.video_views,
            int(stats.video_views_50_quartile),
            int(stats.video_views_100_quartile),
        )

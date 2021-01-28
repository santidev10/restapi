import logging
from utils.utils import chunks_generator

from django.conf import settings
from django.contrib.auth import get_user_model
from django.db.models import Max
from django.db.models import Sum

from aw_reporting.models import VideoCreativeStatistic
from email_reports.reports.abstract_daily_apex_email_report import AbstractDailyApexEmailReport
from email_reports.reports.abstract_daily_apex_email_report import NoStatsDataException
from email_reports.models import VideoCreativeData
from es_components.constants import Sections
from es_components.managers import VideoManager
from utils.youtube_api import resolve_videos_info

logger = logging.getLogger(__name__)

DATE_FORMAT = "%m/%d/%y"
YOUTUBE_LINK_TEMPLATE = "https://www.youtube.com/watch?v={}"
TITLE = "title"


class ApexVisaCreativeDataAggregator:

    def __init__(self, creative_ids: list):
        self.creative_ids = creative_ids
        self.data = {}
        self._init_data()
        self._persist_data()

    def _init_data(self):
        """
        get and create creative data from cache, ES, or API
        :param creative_ids:
        :return:
        """
        unresolved_ids = list(set(self.creative_ids))
        video_creatives = VideoCreativeData.objects.filter(id__in=unresolved_ids)
        postgres_map = {}
        for batch in chunks_generator(video_creatives, size=1000):
            for record in batch:
                postgres_map[record.id] = record.data
        self.extant_postgres_creative_data = postgres_map

        unresolved_ids = list(set(unresolved_ids) - set(postgres_map.keys()))

        manager = VideoManager(Sections.GENERAL_DATA)
        elasticsearch_map = {}
        for video in manager.get(ids=unresolved_ids, skip_none=True):
            elasticsearch_map[video.main.id] = video.to_dict()

        unresolved_ids = list(set(unresolved_ids) - set(elasticsearch_map.keys()))
        youtube_api_data = resolve_videos_info(unresolved_ids) if unresolved_ids else {}

        self.data = {**postgres_map, **elasticsearch_map, **youtube_api_data}

    def _persist_data(self):
        """
        persist data to postgres if needed
        :return:
        """
        cache_records_to_create = []
        for id, data in self.data.items():
            # only create a Postgres cache record if it doesn't already exist in the db,
            # and the data we have contains general_data > title
            if self.extant_postgres_creative_data.get(id, None) is None \
                    and self._data_is_valid(data):
                sanitized_data = self._sanitize_data(data)
                cache_record = VideoCreativeData(id=id, data=sanitized_data)
                cache_records_to_create.append(cache_record)
        VideoCreativeData.objects.bulk_create(cache_records_to_create)

    @staticmethod
    def _data_is_valid(data: dict) -> bool:
        """
        check if the data is valid to be saved
        :param data:
        :return:
        """
        return True if data.get(Sections.GENERAL_DATA, {}).get(TITLE) else False

    @staticmethod
    def _sanitize_data(data: dict) -> dict:
        """
        trim down the data to just what we need
        :param data:
        :return:
        """
        return {
            Sections.GENERAL_DATA: {
                TITLE: data.get(Sections.GENERAL_DATA, {}).get(TITLE)
            }
        }

    def get(self, creative_id: str, default=None):
        """
        get creative data, if any, given a creative id, optionally return a default value if nothing found
        :param creative_id:
        :param default: default value to pass back if no data is found
        :return:
        """
        return self.data.get(creative_id, default)


class DailyApexVisaCampaignEmailReport(AbstractDailyApexEmailReport):

    CAMPAIGNS_FIELDS = ("id", "account__name", "account__currency_code", "salesforce_placement__ordered_rate",
                        "salesforce_placement__goal_type_id", "salesforce_placement__opportunity__ias_campaign_name")
    STATS_FIELDS = ("date", "impressions", "clicks", "video_views_100_quartile", "video_views_50_quartile",
                    "video_views")
    CSV_HEADER = ("Date", "Advertiser Currency", "Device Type", "Campaign ID", "Campaign", "Creative ID",
                  "Creative", "Creative Source", "Revenue (Adv Currency)", "Impressions", "Clicks", "TrueView: Views",
                  "Midpoint Views (Video)", "Complete Views (Video)")

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.creative_data_aggregator = None

    def get_user(self):
        return get_user_model().objects.filter(email=settings.DAILY_APEX_CAMPAIGN_REPORT_CREATOR).first()

    def get_from_email(self):
        return settings.EXPORTS_EMAIL_ADDRESS

    def get_to_list(self):
        return settings.DAILY_APEX_REPORT_EMAIL_ADDRESSES

    def get_cc_list(self):
        return settings.DAILY_APEX_REPORT_CC_EMAIL_ADDRESSES

    def get_historical_filename(self) -> str:
        return "apex_visa_historical.csv"

    def get_attachment_filename(self) -> str:
        return "daily_campaign_report.csv"

    def _get_subject(self):
        return f"Daily Campaign Report for {self.yesterday}"

    def _get_body(self):
        return f"Daily Campaign Report for {self.yesterday}. \nPlease see attached file."

    def get_rows(self, is_historical=False):
        """
        OVERRIDES AbstractDailyApexEmailReport to get ApexVisaCreativeData
        :param is_historical:
        :return:
        """
        campaign_ids = self._get_campaign_ids()
        stats = self.get_stats(campaign_ids, is_historical=is_historical)
        if not stats.exists():
            raise NoStatsDataException()

        creative_ids = self._get_creative_ids_from_stats(stats)
        self.creative_data_aggregator = ApexVisaCreativeDataAggregator(creative_ids=creative_ids)
        rows = self.get_rows_from_stats(list(stats))
        return rows

    # TODO type hints
    @ staticmethod
    def _get_creative_ids_from_stats(stats):
        return [stat.creative_id for stat in stats]

    @staticmethod
    def get_campaign_name(account_name):
        """
        gets apex campaign name substitutions
        :param account_name:
        :return:
        """
        return settings.APEX_CAMPAIGN_NAME_SUBSTITUTIONS.get(account_name, None)

    def get_stats(self, campaign_ids: list, is_historical: bool = False):
        """
        get stats day-by-day, instead of a summed "running total". If
        is_historical is set, then results are not constrained to only
        yesterday's.
        """
        filter_kwargs = {"ad_group__campaign__id__in": campaign_ids, }
        if not is_historical:
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

    def get_rows_from_stats(self, creative_statistics: list):
        """
        overrides AbstractDailyApexEmailReport, does not match signature
        :param creative_statistics:
        :return:
        """
        rows = []
        for stats in creative_statistics:
            ias_campaign_name = stats.ad_group__campaign__salesforce_placement__opportunity__ias_campaign_name
            creative_data = self.creative_data_aggregator.get(stats.creative_id, dict())
            rows.append([
                stats.date.strftime(DATE_FORMAT),
                "EUR",  # stats.ad_group__campaign__account__currency_code,
                "Cross Device",
                stats.ad_group__campaign__id,
                ias_campaign_name or self.get_campaign_name(stats.ad_group__campaign__account__name),
                stats.creative_id,
                creative_data.get(Sections.GENERAL_DATA, {}).get(TITLE),
                YOUTUBE_LINK_TEMPLATE.format(stats.creative_id),
                self._get_revenue(stats, "ad_group__campaign__"),
                *self._get_stats_metrics(stats)
            ])
        return rows

    @staticmethod
    def _get_stats_metrics(stats):
        return (
            stats.impressions,
            stats.clicks,
            stats.video_views,
            int(stats.video_views_50_quartile),
            int(stats.video_views_100_quartile),
        )

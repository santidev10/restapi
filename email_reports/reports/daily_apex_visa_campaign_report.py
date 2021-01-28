import abc
import csv
import logging
import os
from datetime import timedelta
from io import StringIO
from typing import Union
from utils.utils import chunks_generator

from django.conf import settings
from django.contrib.auth import get_user_model
from django.core.mail import EmailMessage
from django.db.models import Max
from django.db.models import Sum

from aw_reporting.models import Campaign
from aw_reporting.models import SalesForceGoalType
from aw_reporting.models import VideoCreativeStatistic
from email_reports.reports.base import BaseEmailReport
from email_reports.models import VideoCreativeData
from es_components.constants import Sections
from es_components.managers import VideoManager
from userprofile.constants import UserSettingsKey
from utils.datetime import now_in_default_tz
from utils.youtube_api import resolve_videos_info

logger = logging.getLogger(__name__)

DATE_FORMAT = "%m/%d/%y"
YOUTUBE_LINK_TEMPLATE = "https://www.youtube.com/watch?v={}"


# TODO rename
class ApexVisaCreativeData:

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
        return True if data.get(Sections.GENERAL_DATA, {}).get("title") else False

    @staticmethod
    def _sanitize_data(data: dict) -> dict:
        """
        trim down the data to just what we need
        :param data:
        :return:
        """
        return {
            Sections.GENERAL_DATA: {
                "title": data.get(Sections.GENERAL_DATA, {}).get("title")
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


class AbstractDailyApexEmailReport(BaseEmailReport):

    def __init__(self, *args, **kwargs):
        """
        is_historical: Bool: If True, fetches ALL VideoCreativeStatistic records
        for the given Accounts ids, rather than just the previous day's
        """
        super().__init__(*args, **kwargs)

        self.today = now_in_default_tz().date()
        self.yesterday = self.today - timedelta(days=1)
        self.user = self.get_user()
        self.from_email = self.get_from_email()
        self.to = self.get_to_list()
        self.cc = self.get_cc_list()
        self.attachment_filename = self.get_attachment_filename()
        self.historical_filename = self.get_historical_filename()

    @abc.abstractmethod
    def get_attachment_filename(self) -> str:
        """ set the attachment filename when using the send() method """
        raise NotImplementedError

    @abc.abstractmethod
    def get_historical_filename(self) -> str:
        """ set the historical filename (to write locally) when using the historical() method """
        raise NotImplementedError

    @abc.abstractmethod
    def get_user(self):
        """ set the user from which to get visible_accounts """
        raise NotImplementedError

    def get_from_email(self):
        """ set the from email when using the send() method """
        raise NotImplementedError

    @abc.abstractmethod
    def get_to_list(self):
        """ set the list of recipients when using the send() method """
        raise NotImplementedError

    @abc.abstractmethod
    def get_cc_list(self):
        raise NotImplementedError

    @abc.abstractmethod
    def _get_subject(self):
        raise NotImplementedError

    @abc.abstractmethod
    def _get_body(self):
        raise NotImplementedError

    @abc.abstractmethod
    def get_stats(self, campaign_ids: list, is_historical: bool = False):
        raise NotImplementedError

    @abc.abstractmethod
    def get_rows_from_stats(self, stats):
        raise NotImplementedError

    def get_account_ids(self):
        """
        get the visible accounts for the supplied user
        :return:
        """
        return self.user.aw_settings.get(UserSettingsKey.VISIBLE_ACCOUNTS)

    def _get_campaign_ids(self):
        """
        get all campaigns that belong to the set user's visible_accounts account ids
        :return:
        """
        account_ids = self.get_account_ids()
        campaigns = Campaign.objects.filter(account_id__in=account_ids) \
            .values_list("id", flat=True)
        campaigns_ids = list(campaigns)
        return campaigns_ids

    def get_rows(self, is_historical=False):
        """
        TODO APEX Visa overrides this
        :param is_historical:
        :return:
        """
        campaign_ids = self._get_campaign_ids()
        stats = self.get_stats(campaign_ids, is_historical=is_historical)
        # stats = self.get_stats(campaign_ids)
        if not stats.exists():
            # TODO raise an exception here?
            print("no stats from which to create a historical. Stopping.")
            return

        rows = self.get_rows_from_stats(list(stats))
        return rows

    def historical(self):
        """
        write a historical report to the local filesystem. DOES NOT SEND email report
        write historical csv data locally to self.historical_filename
        :return:
        """
        rows = self.get_rows(is_historical=True)

        try:
            os.remove(self.historical_filename)
        except FileNotFoundError:
            pass
        with open(self.historical_filename, mode="w") as f:
            writer = csv.writer(f)
            writer.writerow(self.CSV_HEADER)
            writer.writerows(rows)
        print(f"wrote historical data to filename: {self.historical_filename}")

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

        rows = self.get_rows()
        csv_file = StringIO()
        writer = csv.writer(csv_file)
        writer.writerow(self.CSV_HEADER)
        writer.writerows(rows)
        csv_context = csv_file.getvalue()
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


# class DailyApexVisaCampaignEmailReport(BaseEmailReport):
class DailyApexVisaCampaignEmailReport(AbstractDailyApexEmailReport):

    CAMPAIGNS_FIELDS = ("id", "account__name", "account__currency_code", "salesforce_placement__ordered_rate",
                        "salesforce_placement__goal_type_id", "salesforce_placement__opportunity__ias_campaign_name")
    STATS_FIELDS = ("date", "impressions", "clicks", "video_views_100_quartile", "video_views_50_quartile",
                    "video_views")
    CSV_HEADER = ("Date", "Advertiser Currency", "Device Type", "Campaign ID", "Campaign", "Creative ID",
                  "Creative", "Creative Source", "Revenue (Adv Currency)", "Impressions", "Clicks", "TrueView: Views",
                  "Midpoint Views (Video)", "Complete Views (Video)")
    # attachment_filename = "daily_campaign_report.csv"
    # historical_filename = "apex_visa_historical.csv"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.creative_data_getter = None

        # self.today = now_in_default_tz().date()
        # self.yesterday = self.today - timedelta(days=1)
        # self.user = get_user_model().objects.filter(email=settings.DAILY_APEX_CAMPAIGN_REPORT_CREATOR).first()
        # self.from_email = settings.EXPORTS_EMAIL_ADDRESS
        # self.to = settings.DAILY_APEX_REPORT_EMAIL_ADDRESSES
        # self.cc = settings.DAILY_APEX_REPORT_CC_EMAIL_ADDRESSES

    def get_user(self):
        return get_user_model().objects.filter(email=settings.DAILY_APEX_CAMPAIGN_REPORT_CREATOR).first()

    def get_from_email(self):
        return settings.EXPORTS_EMAIL_ADDRESS

    def get_to_list(self):
        # TODO remove
        return ["andrew.wong@channelfactory.com",]
        # return settings.DAILY_APEX_REPORT_EMAIL_ADDRESSES

    def get_cc_list(self):
        # TODO remove
        return []
        # return settings.DAILY_APEX_REPORT_CC_EMAIL_ADDRESSES

    def get_historical_filename(self) -> str:
        return "apex_visa_historical.csv"

    def get_attachment_filename(self) -> str:
        return "daily_campaign_report.csv"

    def _get_subject(self):
        return f"Daily Campaign Report for {self.yesterday}"

    def _get_body(self):
        return f"Daily Campaign Report for {self.yesterday}. \nPlease see attached file."

    # def send(self):
    #     """
    #     used by automated task to send the daily report to defined recipients
    #     :return:
    #     """
    #     if not self.to:
    #         logger.error(f"No recipients set for {self.__class__.__name__} Apex campaign report")
    #         return
    #
    #     if not isinstance(self.user, get_user_model()):
    #         return
    #
    #     csv_context = self._get_csv_file_context()
    #     if not csv_context:
    #         logger.error(f"No data to send {self.__class__.__name__} Apex campaign report.")
    #         return
    #
    #     msg = EmailMessage(
    #         subject=self._get_subject(),
    #         body=self._get_body(),
    #         from_email=self.from_email,
    #         to=self.get_to(self.to),
    #         cc=self.get_cc(self.cc),
    #         bcc=self.get_bcc()
    #     )
    #
    #     msg.attach(self.attachment_filename, csv_context, "text/csv")
    #     msg.send(fail_silently=False)

    # def get_account_ids(self):
    #     return self.user.aw_settings.get(UserSettingsKey.VISIBLE_ACCOUNTS)

    # def _get_campaign_ids(self):
    #     account_ids = self.get_account_ids()
    #     campaigns = Campaign.objects.filter(account_id__in=account_ids) \
    #         .values_list("id", flat=True)
    #     campaigns_ids = list(campaigns)
    #     return campaigns_ids

    def get_rows(self, is_historical=False):
        """
        OVERRIDES AbstractDailyApexEmailReport to get ApexVisaCreativeData
        :param is_historical:
        :return:
        """
        campaign_ids = self._get_campaign_ids()
        # TODO set back to historical=True
        stats = self.get_stats(campaign_ids, is_historical=is_historical)
        # stats = self.get_stats(campaign_ids)
        if not stats.exists():
            # TODO raise an exception here?
            print("no stats from which to create a historical. Stopping.")
            return

        creative_ids = self._get_creative_ids_from_stats(stats)
        self.creative_data_getter = ApexVisaCreativeData(creative_ids=creative_ids)
        rows = self.get_rows_from_stats(list(stats))
        return rows

    # TODO type hints
    @ staticmethod
    def _get_creative_ids_from_stats(stats):
        return [stat.creative_id for stat in stats]

    # def historical(self):
    #     """
    #     write historical csv data locally to self.historical_filename
    #     :return:
    #     """
    #     campaign_ids = self._get_campaign_ids()
    #     # TODO set back to historical=True
    #     stats = self.get_stats(campaign_ids, is_historical=True)
    #     # stats = self.get_stats(campaign_ids)
    #     if not stats.exists():
    #         print("no stats from which to create a historical. Stopping.")
    #         return
    #     creative_ids = self._get_creative_ids_from_stats(stats)
    #     creative_data = ApexVisaCreativeData(creative_ids=creative_ids)
    #     rows = self.get_rows_from_stats(stats, creative_data)
    #     # creative_ids = self._get_creative_ids_from_stats(stats)
    #     # creatives_data = self._get_creative_info()
    #
    #     try:
    #         os.remove(self.historical_filename)
    #     except FileNotFoundError:
    #         pass
    #     with open(self.historical_filename, mode="w") as f:
    #         writer = csv.writer(f)
    #         writer.writerow(self.CSV_HEADER)
    #         # writer.writerows(self.get_rows_from_stats(stats))
    #         writer.writerows(rows)
    #     print(f"wrote historical data to filename: {self.historical_filename}")

    # def _get_csv_file_context(self):
    #     """
    #     get csv data to be sent in daily email
    #     :return:
    #     """
    #     campaign_ids = self._get_campaign_ids()
    #     stats = self.get_stats(campaign_ids)
    #     if not stats.exists():
    #         return None
    #     # TODO add updated stats block here
    #
    #     csv_file = StringIO()
    #     writer = csv.writer(csv_file)
    #     writer.writerow(self.CSV_HEADER)
    #     writer.writerows(self.get_rows_from_stats(stats))
    #     return csv_file.getvalue()

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

    # TODO update inheritor!!!
    def get_rows_from_stats(self, creative_statistics: list):
        """
        overrides AbstractDailyApexEmailReport, does not match signature
        :param creative_statistics:
        :return:
        """
        rows = []
        for stats in creative_statistics:
            ias_campaign_name = stats.ad_group__campaign__salesforce_placement__opportunity__ias_campaign_name
            creative_data = self.creative_data_getter.get(stats.creative_id, dict())
            rows.append([
                stats.date.strftime(DATE_FORMAT),
                "EUR",  # stats.ad_group__campaign__account__currency_code,
                "Cross Device",
                stats.ad_group__campaign__id,
                ias_campaign_name or self.get_campaign_name(stats.ad_group__campaign__account__name),
                stats.creative_id,
                creative_data.get(Sections.GENERAL_DATA, {}).get("title"),
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

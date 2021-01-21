import logging
from typing import Union
from urllib import parse

from django.conf import settings
from django.contrib.auth import get_user_model

from aw_reporting.models import AdStatistic
from email_reports.reports.daily_apex_visa_campaign_report import DailyApexVisaCampaignEmailReport
from email_reports.reports.daily_apex_visa_campaign_report import DATE_FORMAT

logger = logging.getLogger(__name__)

DISNEY_CREATIVE_ID_KEY = "dc_trk_cid"


class DailyApexDisneyCampaignEmailReport(DailyApexVisaCampaignEmailReport):

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

    def get_stats(self, campaign_ids: list, is_historical: bool = False):
        """
        get stats day-by-day, instead of a summed "running total". If
        is_historical is set, then results are not constrained to only
        yesterday's.
        """
        filter_kwargs = {"ad__ad_group__campaign__id__in": campaign_ids, }
        if not is_historical:
            filter_kwargs["date"] = self.yesterday

        return AdStatistic.objects.filter(**filter_kwargs) \
            .order_by("date", "impressions") \
            .values_list(
                "ad__ad_group__campaign__salesforce_placement__apex_go_client_rate",
                "ad__ad_group__campaign__salesforce_placement__goal_type_id",
                "ad__creative_tracking_url_template",
                # stats fields
                "clicks",
                "cost",
                "date",
                "impressions",
                "video_views",
                "video_views_100_quartile",
                "video_views_25_quartile",
                "video_views_50_quartile",
                "video_views_75_quartile",
                named=True)

    def get_rows_from_stats(self, creative_statistics):
        rows = []
        creative_statistics = list(creative_statistics)

        for stats in creative_statistics:
            # shows empty cell if revenue not calculable
            media_cost = self._get_revenue(
                stats,
                "ad__ad_group__campaign__",
                "ad__ad_group__campaign__salesforce_placement__apex_go_client_rate")

            # get DISNEY AD SERVER campaign, placement, creative ids (these are not our values)
            campaign_id, placement_id, creative_id = self._get_disney_ids(stats)

            rows.append([
                None,  # Campaign Advertiser ID (blank for v1)
                None,  # Campaign Advertiser [name] (blank for v1)
                campaign_id,
                None,  # Campaign Name (blank for v1)
                placement_id,
                None,  # Placement Name (blank for v1)
                creative_id,
                None,  #  Creative Name (blank for v1)
                stats.date.strftime(DATE_FORMAT),  # Date
                "GBP",  # Currency
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

    @staticmethod
    def _get_disney_ids(stats):
        """
        get parsed disney ids from an Ad's creative_tracking_url_template
        :param stats:
        :return:
        """
        url = stats.ad__creative_tracking_url_template
        if not url:
            return None, None, None
        parser = TrackingUrlTemplateDisneyIdParser(url)
        return parser.get_campaign_id(), parser.get_placement_id(), parser.get_creative_id()


class TrackingUrlTemplateDisneyIdParser:
    """
    Utility class to parse Disney ids from an Ad's creative_tracking_url_template
    """

    def __init__(self, url: str):
        self.url = url
        self.parse_result = parse.urlparse(url)

    def _is_integer(self, value: Union[str, None]) -> Union[str, None]:
        """
        check that the passed string id can be cast to an integer. return None if not
        also returns none if no value
        :param value:
        :return:
        """
        if not value:
            return None
        try:
            int(value)
        except ValueError:
            return None
        return value

    def _get_campaign_placement_path_part(self) -> Union[str, None]:
        """
        given a ParseResult instantiated from an Ad's creative_tracking_url_template, parse out the raw part
        that contains the campaign and placement id
        value should look like: B24747908.284532709
        :param parse_result:
        :return:
        """
        if hasattr(self, "campaign_placement_path_part"):
            return self.campaign_placement_path_part

        path_parts = self.parse_result.path.split("/")
        candidates = [part for part in path_parts if part.startswith("B") and len(part.split(".")) == 2]
        self.campaign_placement_path_part = candidates[-1] if candidates else None
        return self.campaign_placement_path_part

    def get_campaign_id(self) -> Union[str, None]:
        """
        given a ParseResult instantiated from an Ad's creative_tracking_url_template, parse the campaign id
        :param parse_result:
        :return:
        """
        raw = self._get_campaign_placement_path_part()
        if not raw:
            return None
        return self._is_integer(raw.split(".")[0].strip("B"))

    def get_placement_id(self) -> Union[str, None]:
        """
        given a ParseResult instantiated from an Ad's creative_tracking_url_template, parse the placement id
        :param parse_result:
        :return:
        """
        raw = self._get_campaign_placement_path_part()
        if not raw:
            return None
        return self._is_integer(raw.split(".")[-1])

    def get_creative_id(self) -> Union[str, None]:
        """
        given a ParseResult instantiated from an Ad's creative_tracking_url_template, parse the creative id
        :param parse_result:
        :return:
        """
        params = self.parse_result.params
        if not params:
            return None
        params_dict = dict(parse.parse_qsl(params))
        return params_dict.get(DISNEY_CREATIVE_ID_KEY, None)

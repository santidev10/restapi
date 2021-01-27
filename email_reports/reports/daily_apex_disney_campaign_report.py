import csv
import enum
import logging
from typing import Union
from urllib import parse
from urllib.parse import ParseResult

from django.conf import settings
from django.contrib.auth import get_user_model

from aw_reporting.models import AdStatistic
from email_reports.reports.daily_apex_visa_campaign_report import DATE_FORMAT
from email_reports.reports.daily_apex_visa_campaign_report import DailyApexVisaCampaignEmailReport
from utils.aws.s3_exporter import S3Exporter

logger = logging.getLogger(__name__)

DISNEY_CREATIVE_ID_KEY = "dc_trk_cid"

MAKE_GOOD = "Make Good"

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
        self.tag_sheet_map = DisneyTagSheetMap()

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
            advertiser_id, advertiser_name = self.tag_sheet_map.get_advertiser_id_and_name(campaign_id=campaign_id,
                                                                                           placement_id=placement_id,
                                                                                           creative_id=creative_id)
            placement_name = self.tag_sheet_map.get_placement_name(placement_id) if placement_id else None

            rows.append([
                advertiser_id,  # Campaign Advertiser ID
                advertiser_name,  # Campaign Advertiser [name]
                campaign_id,
                self.tag_sheet_map.get_campaign_name(campaign_id) if campaign_id else None,  # Campaign Name
                placement_id,
                placement_name,  # Placement Name
                creative_id,
                self.tag_sheet_map.get_creative_name(creative_id) if creative_id else None,  # Creative Name
                stats.date.strftime(DATE_FORMAT),  # Date
                "GBP",  # Currency
                0 if isinstance(placement_name, str) and MAKE_GOOD in placement_name else media_cost,
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
        parser = DisneyTrackingUrlTemplateIdParser(url)
        return parser.get_campaign_id(), parser.get_placement_id(), parser.get_creative_id()


class DisneyTrackingUrlTemplateIdParser:
    """
    Utility class to parse Disney ids from an Ad's creative_tracking_url_template
    """

    def __init__(self, url: str):
        self.url = url
        self._init_parse_result()

    def _init_parse_result(self):
        """
        initialize the parse_result parameter. validates hostname
        :return:
        """
        parse_result = parse.urlparse(self.url)
        if parse_result.hostname != "ad.doubleclick.net":
            self.parse_result = None
            return
        self.parse_result = parse_result

    @staticmethod
    def _is_integer(value: Union[str, None]) -> Union[str, None]:
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
        given a ParseResult loaded from self.campaign_placement_path_part, instantiated from an Ad's
        creative_tracking_url_template, parse out the raw part that contains the campaign and placement id
        value should look like: B24747908.284532709
        :return:
        """
        if hasattr(self, "campaign_placement_path_part"):
            return self.campaign_placement_path_part

        if not self.parse_result or not isinstance(self.parse_result, ParseResult):
            return None
        path_parts = self.parse_result.path.split("/")
        candidates = [part for part in path_parts if part.startswith("B") and len(part.split(".")) == 2]
        self.campaign_placement_path_part = candidates[-1] if candidates else None
        return self.campaign_placement_path_part

    def get_campaign_id(self) -> Union[str, None]:
        """
        given a ParseResult instantiated from an Ad's creative_tracking_url_template, parse the campaign id
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
        if not self.parse_result:
            return None
        params = self.parse_result.params
        if not params:
            return None
        params_dict = dict(parse.parse_qsl(params))
        return params_dict.get(DISNEY_CREATIVE_ID_KEY, None)


class DatoramaTagSheetS3Exporter(S3Exporter):
    """
    S3Exporter for interacting with S3 objects
    """
    bucket_name = settings.AMAZON_S3_DATORAMA_TAG_SHEET_BUCKET_NAME
    export_content_type = "application/CSV"

    @classmethod
    def get_s3_key(cls, name):
        key = name
        return key

    @classmethod
    def list_objects(cls):
        s3 = cls._s3()
        return s3.list_objects_v2(
            Bucket=cls.bucket_name
        )


class TagSheetColumnEnum(enum.Enum):
    """
    maps tag sheet column type to column index
    """
    ADVERTISER_ID = 0
    ADVERTISER_NAME = 1
    CAMPAIGN_ID = 2
    CAMPAIGN_NAME = 3
    PLACEMENT_ID = 4
    PLACEMENT_NAME = 7
    CREATIVE_ID = 14
    CREATIVE_NAME = 15


class DisneyTagSheetMap:
    """
    Utility class to pull in tag sheet from s3 bucket, and map given campaign/placement/creative ids to their names
    as well as to their advertiser id, name
    """

    def __init__(self):
        self.s3 = DatoramaTagSheetS3Exporter()
        self._init_file()
        self._init_empty_maps()
        if hasattr(self, "reader"):
            self._init_maps()

    def _get_tag_sheet_file_name(self) -> Union[str, None]:
        """
        gets the first file name listed in the s3 bucket
        :return:
        """
        objects = self.s3.list_objects()

        contents = objects["Contents"]
        try:
            return contents[0]["Key"]
        except (IndexError, KeyError):
            return

    def _init_file(self):
        """
        initialize the tag sheet from s3
        :return:
        """
        file_name = self._get_tag_sheet_file_name()
        if not file_name:
            logger.error(f"Could not find tag sheet in s3 bucket: {self.s3.bucket_name}!")
            return

        if not file_name.endswith(".csv"):
            logger.error(f"Could not find csv in s3 bucket: {self.s3.bucket_name}!")
            return

        ias_content = self.s3._get_s3_object(name=file_name)
        body = ias_content.get("Body")
        if not body:
            logger.error(f"Could not find 'Body' key in s3 response when initializing tag sheet!")
            return

        lines = body.read().decode("utf-8").splitlines(True)
        self.reader = csv.reader(lines)

    def _init_maps(self):
        """
        initialize maps from the reader
        :return:
        """
        header = next(self.reader)
        for row in self.reader:
            self._map_id_to_name(self.campaign_map, row, TagSheetColumnEnum.CAMPAIGN_ID,
                                 TagSheetColumnEnum.CAMPAIGN_NAME)
            self._map_id_to_name(self.placement_map, row, TagSheetColumnEnum.PLACEMENT_ID,
                                 TagSheetColumnEnum.PLACEMENT_NAME)
            self._map_id_to_name(self.creative_map, row, TagSheetColumnEnum.CREATIVE_ID,
                                 TagSheetColumnEnum.CREATIVE_NAME)
            # create a campaign/placement/creative to advertiser map, since we might not have advertiser for a certain
            # creative, but may have it for the placement or campaign
            self._map_id_to_advertiser(self.campaign_advertiser_map, row, TagSheetColumnEnum.CAMPAIGN_ID)
            self._map_id_to_advertiser(self.placement_advertiser_map, row, TagSheetColumnEnum.PLACEMENT_ID)
            self._map_id_to_advertiser(self.creative_advertiser_map, row, TagSheetColumnEnum.CREATIVE_ID)

    def _map_id_to_advertiser(self, map_item: dict, row: list, id_enum: TagSheetColumnEnum):
        """
        maps an advertiser id and name to a given id in a given map item
        :param map_item:
        :param row:
        :param id_enum:
        :return:
        """
        id = self._get_column_value(row=row, enum_obj=id_enum)
        advertiser_id = self._get_column_value(row, TagSheetColumnEnum.ADVERTISER_ID)
        advertiser_name = self._get_column_value(row, TagSheetColumnEnum.ADVERTISER_NAME)
        if not id:
            return
        map_item[id] = (advertiser_id, advertiser_name)

    def _map_id_to_name(self, map_item: dict, row: list, id_enum: TagSheetColumnEnum, name_enum: TagSheetColumnEnum):
        """
        given a map item, row, id key, and name key, store an id keyed to a name
        :param map_item:
        :param row:
        :param id_enum:
        :param name_enum:
        :return:
        """
        id = self._get_column_value(row=row, enum_obj=id_enum)
        name = self._get_column_value(row=row, enum_obj=name_enum)
        if not id:
            return
        map_item[id] = name

    @staticmethod
    def _get_column_value(row: list, enum_obj: TagSheetColumnEnum) -> Union[str, None]:
        """
        given a row and a TagSheetColumnEnum, get the appropriate value from the row
        :param key:
        :return:
        """
        index = enum_obj.value
        try:
            value = row[index]
        except IndexError:
            return None
        return value

    def _init_empty_maps(self):
        """
        initialize empty maps for the campaign, placement, and creative fields.
        :return:
        """
        self.campaign_map = {}
        self.placement_map = {}
        self.creative_map = {}
        # maps each campaign/placement/creative to its advertiser data
        self.campaign_advertiser_map = {}
        self.placement_advertiser_map = {}
        self.creative_advertiser_map = {}

    def get_campaign_name(self, campaign_id: str):
        """
        given an id, get the mapped campaign name
        :param campaign_id:
        :return:
        """
        return self.campaign_map.get(campaign_id)

    def get_placement_name(self, placement_id: str):
        """
        given an id, get the mapped placement name
        :param placement_id:
        :return:
        """
        return self.placement_map.get(placement_id)

    def get_creative_name(self, creative_id: str):
        """
        given an id, get the mapped creative name
        :param creative_id:
        :return:
        """
        return self.creative_map.get(creative_id)

    def get_advertiser_id_and_name(self, campaign_id: str, placement_id: str, creative_id: str):
        """
        given a campaign, placement, and creative id, get the mapped advertiser id, name tuple
        :param creative_id:
        :return:
        """
        default_return_value = (None, None)
        for id, map_item in [(campaign_id, self.campaign_advertiser_map),
                             (placement_id, self.placement_advertiser_map),
                             (creative_id, self.creative_advertiser_map)]:
            if not id:
                continue

            advertiser_id, advertiser_name = map_item.get(id, default_return_value)
            if advertiser_id or advertiser_name:
                return advertiser_id, advertiser_name

        return default_return_value

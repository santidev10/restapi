import logging
from collections import defaultdict
from datetime import timedelta

from django.db.models import DateField
from django.db.models import F
from django.db.models import Sum
from django.db.models.expressions import ExpressionWrapper
from django.db.models.functions import TruncMonth
from django.db.models.functions import TruncYear

from aw_reporting.charts.base_chart import BaseChart
from aw_reporting.charts.base_chart import Indicator
from aw_reporting.charts.base_chart import TOP_LIMIT
from aw_reporting.models import AdGroupStatistic
from aw_reporting.models import AdStatistic
from aw_reporting.models import CampaignHourlyStatistic
from aw_reporting.models import CampaignStatistic
from aw_reporting.models import OpPlacement
from aw_reporting.models import SalesForceGoalType
from aw_reporting.models import VideoCreativeStatistic
from aw_reporting.models import YTChannelStatistic
from aw_reporting.models import YTVideoStatistic
from aw_reporting.models import dict_add_calculated_stats
from aw_reporting.models import dict_norm_base_stats
from aw_reporting.models import dict_quartiles_to_rates
from es_components.constants import Sections
from es_components.managers import ChannelManager
from es_components.managers import VideoManager
from utils.db.functions import TruncQuarter
from utils.db.functions import TruncWeek
from utils.lang import ExtendedEnum
from utils.youtube_api import resolve_videos_info

logger = logging.getLogger(__name__)


class DateSegment(ExtendedEnum):
    DAY = "day"
    WEEK = "week"
    MONTH = "month"
    QUARTER = "quarter"
    YEAR = "year"


class DeliveryChart(BaseChart):
    FIELDS_TO_SERIALIZE = {
        "all_conversions",
        "average_cpm",
        "average_cpv",
        "average_position",
        "clicks",
        "clicks_app_store",
        "clicks_call_to_action_overlay",
        "clicks_cards",
        "clicks_end_cap",
        "clicks_website",
        "conversions",
        "cost",
        "ctr",
        "ctr_v",
        "date_segment",
        "duration",
        "id",
        "impressions",
        "name",
        "status",
        "thumbnail",
        "video100rate",
        "video25rate",
        "video50rate",
        "video75rate",
        "video_clicks",
        "video_view_rate",
        "video_views",
        "view_through",
    }

    def __init__(self, region_ids=None, date_segment=None, **kwargs):
        super(DeliveryChart, self).__init__(**kwargs,
                                            custom_params=dict(region_ids=region_ids,
                                                               date_segment=date_segment)
                                            )

    def _get_planned_data(self):
        return self._get_planned_data_base(default_end=None)

    def get_items(self):
        self.params["date"] = False
        segmented_by = self.params["segmented_by"]
        if segmented_by:
            return self.get_segmented_data(
                self._get_items, segmented_by
            )
        return self._get_items()

    # pylint: disable=too-many-nested-blocks
    def _get_items(self):
        daily_method = getattr(
            self, "_get_%s_data" % self.params["dimension"]
        )
        data = daily_method()
        response = {
            "items": [],
            "summary": defaultdict(float)
        }
        average_positions = []
        for label, stats in data.items():
            if not stats:
                continue
            for stat in stats:
                dict_norm_base_stats(stat)

                for n, v in stat.items():
                    if v is not None \
                        and not isinstance(v, str) \
                        and n != "id":
                        if n == "average_position":
                            average_positions.append(v)
                        elif n in ("date_segment", "date"):
                            pass
                        else:
                            response["summary"][n] += v

                dict_add_calculated_stats(stat)
                dict_quartiles_to_rates(stat)
                del stat["video_impressions"]

                if "label" in stat:
                    stat["name"] = stat["label"]
                    del stat["label"]
                else:
                    stat["name"] = label
                response["items"].append(
                    stat
                )

        dict_add_calculated_stats(response["summary"])
        if "video_impressions" in response["summary"]:
            del response["summary"]["video_impressions"]
        if average_positions:
            response["summary"]["average_position"] = sum(
                average_positions) / len(average_positions)
        dict_quartiles_to_rates(response["summary"])

        top_by, reverse = self.get_top_by()
        response["items"] = sorted(
            response["items"],
            key=lambda i: i[top_by] if i[top_by] else 0,
            reverse=reverse,
        )
        response["items"] = self._serialize_items(response["items"])
        return response
    # pylint: enable=too-many-nested-blocks

    @staticmethod
    def get_ad_group_link(queryset):
        if queryset.model is CampaignStatistic:
            return "campaign__ad_groups"
        if queryset.model is AdStatistic:
            return "ad__ad_group"
        return "ad_group"

    def get_placements(self):
        queryset = OpPlacement.objects.all()
        filters = {
            "adwords_campaigns__account_id__in": self.params["accounts"]
        }
        if self.params["start"]:
            filters["end__gte"] = self.params["start"]
        if self.params["end"]:
            filters["start__lte"] = self.params["end"]

        if self.params["am_ids"] is not None:
            filters["opportunity__account_manager_id__in"] = self.params[
                "am_ids"]

        if self.params["ad_ops_ids"] is not None:
            filters["opportunity__ad_ops_manager_id__in"] = self.params[
                "ad_ops_ids"]

        if self.params["sales_ids"] is not None:
            filters["opportunity__sales_manager_id__in"] = self.params[
                "sales_ids"]

        if self.params["brands"] is not None:
            filters["opportunity__brand__in"] = self.params["brands"]

        if self.params["goal_type_ids"] is not None:
            filters["goal_type_id__in"] = self.params["goal_type_ids"]

        if self.params["category_ids"] is not None:
            filters["opportunity__category_id__in"] = self.params[
                "category_ids"]

        if self.params["region_ids"] is not None:
            filters["opportunity__region_id__in"] = self.params["region_ids"]

        if self.params["apex_deal"] is not None:
            filters["opportunity__apex_deal"] = self.params["apex_deal"]

        indicator = self.params["indicator"]
        if indicator in (Indicator.CPM, Indicator.IMPRESSIONS):
            filters["goal_type_id"] = SalesForceGoalType.CPM
        if indicator in (Indicator.CPV, Indicator.VIEWS):
            filters["goal_type_id"] = SalesForceGoalType.CPV

        if filters:
            queryset = queryset.filter(**filters)

        return queryset.distinct()

    def filter_queryset(self, queryset):
        camp_link = self.get_camp_link(queryset)
        opp_link = "%s__salesforce_placement__opportunity" % camp_link
        filters = {"%s__account_id__in" % camp_link: self.params["accounts"]}
        if self.params["start"]:
            filters["date__gte"] = self.params["start"]
        if self.params["end"]:
            filters["date__lte"] = self.params["end"]

        if self.params["ad_groups"]:
            ad_group_link = self.get_ad_group_link(queryset)
            filters["%s__id__in" % ad_group_link] = self.params["ad_groups"]

        if self.params["campaigns"]:
            filters["%s_id__in" % camp_link] = self.params["campaigns"]

        if self.params["indicator"] in (Indicator.CPV, Indicator.CTR_V,
                                        Indicator.VIEW_RATE):
            filters["video_views__gt"] = 0

        if self.params["am_ids"] is not None:
            filters["%s__account_manager_id__in" % opp_link] = self.params[
                "am_ids"]

        if self.params["ad_ops_ids"] is not None:
            filters["%s__ad_ops_manager_id__in" % opp_link] = self.params[
                "ad_ops_ids"]

        if self.params["sales_ids"] is not None:
            filters["%s__sales_manager_id__in" % opp_link] = self.params[
                "sales_ids"]

        if self.params["brands"] is not None:
            filters["%s__brand__in" % opp_link] = self.params["brands"]

        if self.params["goal_type_ids"] is not None:
            filters[
                "%s__salesforce_placement__goal_type_id__in" % camp_link] = \
                self.params["goal_type_ids"]

        if self.params["category_ids"] is not None:
            filters["%s__category_id__in" % opp_link] = self.params[
                "category_ids"]

        if self.params["region_ids"] is not None:
            filters["%s__region_id__in" % opp_link] = self.params["region_ids"]

        if self.params["apex_deal"] is not None:
            filters["%s__apex_deal" % opp_link] = self.params["apex_deal"]

        if filters:
            queryset = queryset.filter(**filters)

        return queryset.model.objects.filter(pk__in=queryset.values_list("pk", flat=True))

    def _get_date_segment(self):
        try:
            return DateSegment(self.params["date_segment"])
        except ValueError:
            return None

    def _get_date_segment_annotations(self):
        date_segment = self._get_date_segment()

        if date_segment == DateSegment.DAY:
            return F("date")
        if date_segment == DateSegment.WEEK:
            to_date = lambda e: ExpressionWrapper(e, output_field=DateField())
            next_date = to_date(F("date") + timedelta(days=1))
            start_of_the_week = TruncWeek(next_date)
            shift_back = to_date(start_of_the_week - timedelta(days=1))
            return shift_back
        if date_segment == DateSegment.MONTH:
            return TruncMonth("date")
        if date_segment == DateSegment.YEAR:
            return TruncYear("date")
        if date_segment == DateSegment.QUARTER:
            return TruncQuarter("date")
        return None

    def _get_campaign_ref(self, queryset):
        model = queryset.model
        if model in (CampaignHourlyStatistic, CampaignStatistic):
            return "campaign"
        return super(DeliveryChart, self)._get_campaign_ref(queryset)

    def get_top_by(self):
        if self.params["indicator"] == Indicator.COST:
            return "cost", True
        if self._get_date_segment() is not None:
            return "date_segment", False
        return "impressions", True

    def get_top_data(self, queryset, key):
        group_by = [key]

        date = self.params["date"]
        if date:
            top_by, _ = self.get_top_by()
            top_data = self.filter_queryset(queryset).values(key).annotate(
                top_by=Sum(top_by)
            ).order_by("-top_by")[:TOP_LIMIT]
            ids = [i[key] for i in top_data]

            queryset = queryset.filter(**{"%s__in" % key: ids})
            stats = self.get_raw_stats(
                queryset, group_by, date=True
            )

        else:
            stats = self.get_raw_stats(
                queryset, group_by
            )

        return stats

    def get_raw_stats(self, queryset, group_by, date=None):
        if date is None:
            date = self.params["date"]
        if date:
            group_by.append("date")

        if self._get_date_segment():
            group_by.append("date_segment")

        queryset = self.filter_queryset(queryset)
        date_segment_annotation = self._get_date_segment_annotations()
        if date_segment_annotation:
            queryset = queryset.annotate(date_segment=date_segment_annotation)
        queryset = queryset.values(*group_by).order_by(*group_by)
        return self.add_annotate(queryset)

    def _get_creative_data(self):
        result = defaultdict(list)
        raw_stats = self.get_raw_stats(
            VideoCreativeStatistic.objects.all(), ["creative_id"],
            date=self.params["date"]
        )
        if raw_stats:
            ids = [s["creative_id"] for s in raw_stats]
            manager = VideoManager(Sections.GENERAL_DATA)
            videos_map = {}
            for video in manager.get(ids=ids, skip_none=True):
                videos_map[video.main.id] = video.to_dict()

            unresolved_ids = list(set(ids) - set(videos_map.keys()))
            unresolved_videos_info = resolve_videos_info(unresolved_ids) if unresolved_ids else {}

            videos_map = {**videos_map, **unresolved_videos_info}

            for item in raw_stats:
                youtube_id = item["creative_id"]
                video = videos_map.get(youtube_id, {})
                item["id"] = youtube_id

                item["thumbnail"] = video.get("general_data", {}).get("thumbnail_image_url")
                item["label"] = video.get("general_data", {}).get("title")
                item["duration"] = video.get("general_data", {}).get("duration")
                del item["creative_id"]
                result[youtube_id].append(item)
        else:
            group_by = ["ad__creative_name"]
            raw_stats = self.get_raw_stats(
                AdStatistic.objects.all(), group_by,
                self.params["date"],
            )
            result = defaultdict(list)
            for item in raw_stats:
                uid = item["ad__creative_name"]
                item["label"] = uid
                del item["ad__creative_name"]
                result[uid].append(item)
        return result

    def _get_overview_data(self):
        group_by = ["ad_group__campaign__account_id", "ad_group__campaign__account__name"]
        raw_stats = self.get_raw_stats(
            AdGroupStatistic.objects.all(), group_by,
            self.params["date"],
        )
        result = defaultdict(list)
        for item in raw_stats:
            uid = "ad_group__campaign__account_id"
            item["label"] = item["ad_group__campaign__account__name"]
            result[uid].append(item)
        return result

    def _get_campaign_data(self):
        group_by = ["campaign_id", "campaign__name"]
        raw_stats = self.get_raw_stats(
            CampaignStatistic.objects.all(), group_by,
            self.params["date"],
        )
        result = defaultdict(list)
        for item in raw_stats:
            uid = item["campaign_id"]
            item["label"] = item["campaign__name"]
            result[uid].append(item)
        return result

    def _get_video_data(self, **_):
        raw_stats = self.get_top_data(
            YTVideoStatistic.objects.all(),
            "yt_id"
        )

        ids = [i["yt_id"] for i in raw_stats]
        manager = VideoManager(Sections.GENERAL_DATA)
        videos_map = {}
        for video in manager.get_or_create(ids=ids):
            videos_map[video.main.id] = video

        result = defaultdict(list)
        for item in raw_stats:
            youtube_id = item["yt_id"]
            del item["yt_id"]
            video = videos_map.get(youtube_id)
            item["id"] = youtube_id
            if video.general_data:
                item["label"] = video.general_data.title
                item["thumbnail"] = video.general_data.thumbnail_image_url
                item["duration"] = video.general_data.duration
            title = video.general_data.title if video.general_data else youtube_id
            result[title].append(item)

        return result

    def _get_channel_data(self):
        raw_stats = self.get_top_data(
            YTChannelStatistic.objects.all(),
            "yt_id",
        )

        ids = [i["yt_id"] for i in raw_stats]
        manager = ChannelManager(Sections.GENERAL_DATA)
        channels_map = {}
        for channel in manager.get_or_create(ids=ids):
            channels_map[channel.main.id] = channel

        result = defaultdict(list)
        for item in raw_stats:
            channel_id = item["yt_id"]
            del item["yt_id"]
            channel = channels_map.get(channel_id)
            item["id"] = channel_id
            if channel.general_data:
                item["thumbnail"] = channel.general_data.thumbnail_image_url
                label = channel.general_data.title
            else:
                label = channel.main.id
            result[label].append(item)
        return result

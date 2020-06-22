import logging
from collections import defaultdict

from django.db.models import Sum

from aw_reporting.charts.base_chart import BaseChart
from aw_reporting.charts.base_chart import Indicator
from aw_reporting.charts.base_chart import TOP_LIMIT
from aw_reporting.models import AdStatistic
from aw_reporting.models import CampaignHourlyStatistic
from aw_reporting.models import OpPlacement
from aw_reporting.models import SalesForceGoalType
from aw_reporting.models import VideoCreativeStatistic
from aw_reporting.models import YTChannelStatistic
from aw_reporting.models import YTVideoStatistic
from aw_reporting.models import dict_add_calculated_stats
from aw_reporting.models import dict_norm_base_stats
from aw_reporting.models import dict_quartiles_to_rates
from es_components.managers.channel import ChannelManager
from es_components.managers.video import VideoManager
from utils.datetime import now_in_default_tz

logger = logging.getLogger(__name__)


class DeliveryChart(BaseChart):
    channel_manager = ChannelManager()
    es_fields_to_load_channel_info = ("main.id", "general_data.title", "general_data.thumbnail_image_url",)

    video_manager = VideoManager()
    es_fields_to_load_video_info = ("main.id", "general_data.title", "general_data.thumbnail_image_url",
                                    "general_data.duration",)
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

    def __init__(self, territories=None, **kwargs):
        super(DeliveryChart, self).__init__(**kwargs,
                                            custom_params=dict(territories=territories))

    def _get_planned_data(self):
        return self._get_planned_data_base(default_end=now_in_default_tz().date())

    def get_items(self):
        self.params["date"] = False
        segmented_by = self.params["segmented_by"]
        if segmented_by:
            return self.get_segmented_data(
                self._get_items, segmented_by
            )
        return self._get_items()

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
            stat = stats[0]
            dict_norm_base_stats(stat)

            for n, v in stat.items():
                if v is not None \
                    and not isinstance(v, str) \
                    and n != "id":
                    if n == "average_position":
                        average_positions.append(v)
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

        top_by = self.get_top_by()
        response["items"] = sorted(
            response["items"],
            key=lambda i: i[top_by] if i[top_by] else 0,
            reverse=True,
        )
        response["items"] = self._serialize_items(response["items"])
        return response

    @staticmethod
    def get_ad_group_link(queryset):
        if queryset.model is AdStatistic:
            return "ad__ad_group"
        return "ad_group"

    def get_placements(self):
        queryset = OpPlacement.objects.filter(start__isnull=False, end__isnull=False)
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

        if self.params["territories"] is not None:
            filters["opportunity__territory__in"] = self.params["territories"]

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
            filters["%s_id__in" % ad_group_link] = self.params["ad_groups"]

        if self.params["campaigns"]:
            filters["%s_id__in" % camp_link] = self.params["campaigns"]

        if self.params["indicator"] in (Indicator.CPV, Indicator.CTR_V,
                                        Indicator.VIEW_RATE):
            filters["video_views__gt"] = 0

        if self.params["am_ids"] is not None:
            filters["%s__account_manager_id__in" % opp_link] = self.params["am_ids"]

        if self.params["ad_ops_ids"] is not None:
            filters["%s__ad_ops_manager_id__in" % opp_link] = self.params["ad_ops_ids"]

        if self.params["sales_ids"] is not None:
            filters["%s__sales_manager_id__in" % opp_link] = self.params["sales_ids"]

        if self.params["brands"] is not None:
            filters["%s__brand__in" % opp_link] = self.params["brands"]

        if self.params["goal_type_ids"] is not None:
            filters[
                "%s__salesforce_placement__goal_type_id__in" % camp_link] = \
                self.params["goal_type_ids"]

        if self.params["category_ids"] is not None:
            filters["%s__category_id__in" % opp_link] = self.params["category_ids"]

        if self.params["territories"] is not None:
            filters["%s__territory__in" % opp_link] = self.params["territories"]

        if self.params["apex_deal"] is not None:
            filters["%s__apex_deal" % opp_link] = self.params["apex_deal"]

        if filters:
            queryset = queryset.filter(**filters)

        return queryset

    def _get_campaign_ref(self, queryset):
        model = queryset.model
        if model is CampaignHourlyStatistic:
            return "campaign"
        return super(DeliveryChart, self)._get_campaign_ref(queryset)

    def get_top_by(self):
        if self.params["indicator"] == Indicator.COST:
            return "cost"
        return "impressions"

    def get_top_data(self, queryset, key):
        group_by = [key]

        date = self.params["date"]
        if date:
            top_by = self.get_top_by()
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
        queryset = self.filter_queryset(queryset)
        queryset = queryset.values(*group_by).order_by(*group_by)
        return self.add_annotate(queryset)

    def _get_creative_data(self):
        result = defaultdict(list)
        raw_stats = self.get_raw_stats(
            VideoCreativeStatistic.objects.all(), ["creative_id"],
            date=self.params["date"]
        )
        if raw_stats:
            try:
                ids = [s["creative_id"] for s in raw_stats]
                items = self.video_manager.search(
                    filters=self.video_manager.ids_query(ids)
                ). \
                    source(includes=list(self.es_fields_to_load_video_info)).execute().hits
            except Exception as e:
                logger.error(e)
                videos_info = {}
            else:
                videos_info = {i.main.id: i for i in items}

            for item in raw_stats:
                youtube_id = item["creative_id"]
                info = videos_info.get(youtube_id)
                item["id"] = youtube_id
                item["label"] = info.general_data.title if info else youtube_id
                item["thumbnail"] = info.general_data.thumbnail_image_url if info else None
                item["duration"] = info.general_data.duration if info else None
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

    def _get_video_data(self, **_):
        raw_stats = self.get_top_data(
            YTVideoStatistic.objects.all(),
            "yt_id"
        )
        try:
            ids = [s["yt_id"] for s in raw_stats]
            items = self.video_manager.search(
                filters=self.video_manager.ids_query(ids)
            ). \
                source(includes=list(self.es_fields_to_load_video_info)).execute().hits
        except Exception as e:
            logger.error(e)
            videos_info = {}
        else:
            videos_info = {i.main.id: i for i in items}

        result = defaultdict(list)
        for item in raw_stats:
            youtube_id = item["yt_id"]
            del item["yt_id"]
            info = videos_info.get(youtube_id)
            item["id"] = youtube_id
            item["label"] = info.general_data.title if info else youtube_id
            item["thumbnail"] = info.general_data.thumbnail_image_url if info else None
            item["duration"] = info.general_data.duration if info else None
            title = item["label"]
            result[title].append(item)
        return result

    def _get_channel_data(self):
        raw_stats = self.get_top_data(
            YTChannelStatistic.objects.all(),
            "yt_id",
        )
        try:
            ids = list(set(s["yt_id"] for s in raw_stats))
            items = self.channel_manager.search(
                filters=self.channel_manager.ids_query(ids)
            ). \
                source(includes=list(self.es_fields_to_load_channel_info)).execute().hits

        except Exception as e:
            logger.error(e)
            channels_info = {}
        else:
            channels_info = {i.main.id: i for i in items}

        result = defaultdict(list)
        for item in raw_stats:
            channel_id = item["yt_id"]
            del item["yt_id"]
            item["id"] = channel_id
            info = channels_info.get(channel_id)
            item["thumbnail"] = info.general_data.thumbnail_image_url if info else None
            label = info.general_data.title if info else channel_id
            result[label].append(item)
        return result

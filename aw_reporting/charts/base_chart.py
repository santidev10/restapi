import logging
from collections import defaultdict
from datetime import datetime
from datetime import timedelta

from django.db.models import Avg
from django.db.models import Case
from django.db.models import F
from django.db.models import FloatField
from django.db.models import Min
from django.db.models import Sum
from django.db.models import When
from django.db.models.sql.query import get_field_names_from_opts

from aw_reporting.calculations.cost import get_client_cost_aggregation
from aw_reporting.models import AdGroupStatistic
from aw_reporting.models import AdStatistic
from aw_reporting.models import AgeRangeStatistic
from aw_reporting.models import AgeRanges
from aw_reporting.models import Audience
from aw_reporting.models import AudienceStatistic
from aw_reporting.models import CALCULATED_STATS
from aw_reporting.models import CLICKS_STATS
from aw_reporting.models import CONVERSIONS
from aw_reporting.models import Campaign
from aw_reporting.models import CampaignHourlyStatistic
from aw_reporting.models import CityStatistic
from aw_reporting.models import GenderStatistic
from aw_reporting.models import Genders
from aw_reporting.models import GeoTarget
from aw_reporting.models import KeywordStatistic
from aw_reporting.models import QUARTILE_STATS
from aw_reporting.models import RemarkList
from aw_reporting.models import RemarkStatistic
from aw_reporting.models import SUM_STATS
from aw_reporting.models import Topic
from aw_reporting.models import TopicStatistic
from aw_reporting.models import VideoCreativeStatistic
from aw_reporting.models import YTChannelStatistic
from aw_reporting.models import YTVideoStatistic
from aw_reporting.models import base_stats_aggregator
from aw_reporting.models import device_str
from aw_reporting.models import dict_norm_base_stats
from aw_reporting.models.ad_words.calculations import all_stats_aggregator
from aw_reporting.models.ad_words.statistic import BaseClicksTypesStatisticsModel
from aw_reporting.utils import get_dates_range
from utils.datetime import as_datetime
from utils.datetime import now_in_default_tz
from utils.lang import flatten
from utils.utils import get_all_class_constants

logger = logging.getLogger(__name__)

TOP_LIMIT = 10


class TrendId:
    HISTORICAL = "historical"
    PLANNED = "planned"


class Indicator:
    CPV = "average_cpv"
    CPM = "average_cpm"
    VIEW_RATE = "video_view_rate"
    CTR = "ctr"
    CTR_V = "ctr_v"
    IMPRESSIONS = "impressions"
    VIEWS = "video_views"
    CLICKS = "clicks"
    COST = "cost"


ALL_INDICATORS = get_all_class_constants(Indicator)


class Dimension:
    ADS = "ad"
    AGE = "age"
    CHANNEL = "channel"
    CREATIVE = "creative"
    DEVICE = "device"
    GENDER = "gender"
    INTEREST = "interest"
    KEYWORD = "keyword"
    LOCATION = "location"
    REMARKETING = "remarketing"
    TOPIC = "topic"
    VIDEO = "video"


ALL_DIMENSIONS = get_all_class_constants(Dimension)

INDICATORS_HAVE_PLANNED = (Indicator.CPM, Indicator.CPV, Indicator.IMPRESSIONS,
                           Indicator.VIEWS, Indicator.COST)


class Breakdown:
    HOURLY = "hourly"
    DAILY = "daily"


class BaseChart:
    FIELDS_TO_SERIALIZE = None

    # pylint: disable=too-many-arguments
    def __init__(self, accounts=None, account=None, campaigns=None, campaign=None,
                 ad_groups=None,
                 indicator=None, dimension=None, breakdown=Breakdown.DAILY,
                 start_date=None, end_date=None,
                 additional_chart=None, segmented_by=None,
                 date=True, am_ids=None, ad_ops_ids=None, sales_ids=None,
                 goal_type_ids=None, brands=None, category_ids=None,
                 with_plan=False, show_aw_costs=False, show_conversions=True,
                 apex_deal=None, custom_params=None, **_):
        if account and account in accounts:
            accounts = [account]

        if campaign:
            campaigns = [campaign]

        if not campaigns and accounts:
            campaigns = Campaign.objects \
                .filter(account_id__in=accounts) \
                .values_list("id", flat=True)

        self.params = dict(
            accounts=accounts,
            campaigns=campaigns,
            ad_groups=ad_groups,
            indicator=indicator,
            dimension=dimension,
            breakdown=breakdown,
            start=start_date,
            end=end_date,
            segmented_by=segmented_by,
            date=date,
            am_ids=am_ids,
            ad_ops_ids=ad_ops_ids,
            sales_ids=sales_ids,
            goal_type_ids=goal_type_ids,
            brands=brands,
            category_ids=category_ids,
            show_aw_costs=show_aw_costs,
            show_conversions=show_conversions,
            apex_deal=apex_deal,
            **(custom_params or {})
        )

        self.with_plan = with_plan

        if additional_chart is None:
            additional_chart = bool(dimension)
        self.additional_chart = additional_chart
        self.additional_chart_type = "pie" if indicator in SUM_STATS and \
                                              dimension in (
                                                  "ad", "age", "gender",
                                                  "creative",
                                                  "device") \
            else "bar"

    # pylint: enable=too-many-arguments

    def get_response(self):
        chart_type_kwargs = dict(
            additional_chart=self.additional_chart,
            additional_chart_type=self.additional_chart_type,
        )
        if self.params["segmented_by"]:
            charts = self.get_segmented_data(
                self.get_chart_data,
                self.params["segmented_by"],
                **chart_type_kwargs
            )
        else:
            charts = [
                dict(
                    id=TrendId.HISTORICAL,
                    title="",
                    data=self.get_chart_data(),
                    **chart_type_kwargs
                )
            ]

            if self.with_plan:
                planned_data = self._get_planned_data()
                if planned_data is not None:
                    charts.append(dict(
                        id=TrendId.PLANNED,
                        title="",
                        data=[planned_data],
                        additional_chart=False,
                        additional_chart_type="bar"
                    ))

        return charts

    # pylint: disable=too-many-nested-blocks
    def get_chart_data(self):
        params = self.params

        dimension = self.params["dimension"]
        method = getattr(self, "_get_%s_data" % dimension, None)
        breakdown = self.params["breakdown"]

        if method:
            items_by_label = method()
        elif breakdown == Breakdown.HOURLY:
            group_by = ("date", "hour")
            data = self.get_raw_stats(
                CampaignHourlyStatistic.objects.all(), group_by, False
            )
            items_by_label = dict(Summary=data)
        else:
            group_by = ["date"]
            data = self.get_raw_stats(
                AdGroupStatistic.objects.all(), group_by
            )
            items_by_label = dict(Summary=data)

        fields = self.get_fields()
        values_func = self.get_values_func()
        chart_items = []

        for label, items in items_by_label.items():
            results = []
            summaries = defaultdict(float)
            for item in items:
                if "label" in item:
                    label = item["label"]
                    del item["label"]

                value = values_func(item)
                if value is not None:
                    if "hour" in item:
                        date = item["date"]
                        point_label = datetime(
                            year=date.year, month=date.month,
                            day=date.day, hour=item["hour"],
                        )
                    else:
                        point_label = item["date"]

                    results.append(
                        {
                            "label": point_label,
                            "value": value
                        }
                    )
                    for f in fields:
                        v = item[f]
                        if v:
                            summaries[f] += v

            # if not empty chart
            if any(i["value"] for i in results):
                value = values_func(summaries)
                days = len(results)
                average = (value / days
                           if params["indicator"] in SUM_STATS and days
                           else value)

                chart_item = dict(
                    value=values_func(summaries),
                    trend=results,
                    average=average,
                    label=label
                )
                chart_items.append(
                    chart_item
                )

        if params["indicator"] in SUM_STATS:
            self.fill_missed_dates(chart_items)

        # sort by label
        chart_items = sorted(chart_items, key=lambda i: i["label"])

        return chart_items
    # pylint: enable=too-many-nested-blocks

    def get_raw_stats(self, queryset, group_by, date=None):
        raise NotImplementedError

    def add_annotate(self, queryset):
        if not self.params["date"]:
            kwargs = dict(**all_stats_aggregator())
            if queryset.model is AdStatistic:
                kwargs["average_position"] = Avg(
                    Case(
                        When(
                            average_position__gt=0,
                            then=F("average_position"),
                        ),
                        output_field=FloatField(),
                    )
                )

        else:
            kwargs = {}
            fields = self.get_fields()
            all_sum_stats = SUM_STATS + CONVERSIONS + QUARTILE_STATS
            base_stats_aggregate = base_stats_aggregator()
            for v in fields:
                if v in all_sum_stats:
                    kwargs["sum_%s" % v] = Sum(v)
                elif v in base_stats_aggregate:
                    kwargs[v] = base_stats_aggregate[v]

        if not self.params["show_aw_costs"]:
            campaign_ref = self._get_campaign_ref(queryset)
            kwargs["sum_cost"] = get_client_cost_aggregation(campaign_ref)
        if not self.params["show_conversions"]:
            for key in CONVERSIONS:
                del kwargs["sum_{}".format(key)]
        if issubclass(queryset.model, BaseClicksTypesStatisticsModel):
            for field in CLICKS_STATS:
                kwargs["sum_{}".format(field)] = Sum(field)
        return queryset.annotate(**kwargs)

    def get_segmented_data(self, method, segmented_by, **kwargs):
        items = defaultdict(lambda: {"campaigns": []})
        if self.params["ad_groups"]:
            qs = Campaign.objects \
                .filter(ad_groups__id__in=self.params["ad_groups"], ) \
                .distinct()
        elif self.params["campaigns"]:
            qs = Campaign.objects \
                .filter(pk__in=self.params["campaigns"], )
        else:
            qs = Campaign.objects.none()

        for i in qs.values("id", "name"):
            item = items[i["id"]]
            item["name"] = i["name"]
            item["campaigns"].append(i["id"])

        result = []
        if len(items) > 1:  # summary for >1 items
            sum_key = "Summary for %d %s" % (len(items), segmented_by)
            result.append(
                dict(
                    title=sum_key,
                    data=method(),
                    **kwargs
                )
            )

        items = sorted(items.values(), key=lambda n: n["name"])
        for i in items:
            self.params["campaigns"] = i["campaigns"]
            result.append(
                dict(
                    title=i["name"],
                    data=method(),
                    **kwargs
                )
            )
        return result

    def _get_planned_data(self):
        raise NotImplementedError

    def _get_planned_data_base(self, default_end=None):
        if self.params.get("indicator") not in INDICATORS_HAVE_PLANNED:
            return None

        placements = self.get_placements()
        placements_start = placements.values("start").aggregate(Min("start"))["start__min"]

        if placements_start is None:
            return None

        placements = placements.values("start",
                                       "end",
                                       "total_cost",
                                       "ordered_units")

        start = placements_start
        if self.params.get("start") is not None and self.params.get("start") > start:
            start = self.params.get("start")
        end = self.params.get("end")
        if end is None:
            end = default_end

        total_days = (end - start).days + 1
        trend = [
            self._plan_value_for_date(placements, start + timedelta(days=i))
            for i in range(total_days)
        ]
        value = sum([r.get("value") for r in trend]) if trend else None
        value = value / total_days if value is not None and total_days else 0
        breakdown = self.params["breakdown"]
        if breakdown == Breakdown.HOURLY:
            trend = flatten([self._extend_to_day(i) for i in trend])
        return dict(
            value=value,
            average=value,
            trend=trend,
            label="Planned"
        )

    def get_fields(self):

        if not self.params["date"]:
            item_stats = SUM_STATS + QUARTILE_STATS
            return item_stats

        indicator = self.params["indicator"]

        if indicator in CALCULATED_STATS:
            info = CALCULATED_STATS[indicator]
            fields = info["args"]

        elif indicator in SUM_STATS:
            fields = (indicator,)
        else:
            raise ValueError("Unexpected indicator: %s" % indicator)

        return fields

    def get_values_func(self):
        indicator = self.params["indicator"]

        if indicator in CALCULATED_STATS:
            info = CALCULATED_STATS[indicator]
            receipt = info["receipt"]

            def value_func(data):
                dict_norm_base_stats(data)
                return receipt(**data)

        elif indicator in SUM_STATS:
            def value_func(data):
                dict_norm_base_stats(data)
                return data.get(indicator)
        else:
            raise ValueError("Unexpected indicator: %s" % indicator)
        return value_func

    # pylint: disable=too-many-nested-blocks
    @staticmethod
    def fill_missed_dates(init_data):
        prev_date = None
        for element in init_data:
            trend = []
            for item in element["trend"]:
                curr_date = item["label"]
                if prev_date:
                    days = (curr_date - prev_date).days
                    if days > 1:
                        dates_gen = get_dates_range(prev_date, curr_date)
                        for date in list(dates_gen)[1:-1]:
                            trend.append(
                                {
                                    "label": date,
                                    "value": 0
                                }
                            )
                trend.append(item)
                prev_date = curr_date

            element["trend"] = trend
    # pylint: enable=too-many-nested-blocks

    def _get_campaign_ref(self, queryset):
        model = queryset.model
        if model is AdStatistic:
            return "ad__ad_group__campaign"
        if model in (AgeRangeStatistic, AdGroupStatistic, GenderStatistic,
                     TopicStatistic, CityStatistic, KeywordStatistic,
                     RemarkStatistic, VideoCreativeStatistic,
                     AudienceStatistic, YTChannelStatistic, YTVideoStatistic):
            return "ad_group__campaign"
        logger.error("Undefined model %s", model)
        raise NotImplementedError

    def _plan_value_for_date(self, placements, date):
        values = [self._plan_placement_value_for_date(p, date) for p in
                  placements]
        numerators = [val[0] for val in values]
        denominators = [val[1] if len(val) > 1 else 0 for val in values]

        numerator = sum(numerators) if numerators else 0
        denominator = sum(denominators) if denominators else 1

        if denominator == 0:
            denominator = 1

        return dict(value=numerator / denominator,
                    label=date)

    def get_placements(self):
        raise NotImplementedError

    def _extend_to_day(self, item):
        divider = 1 \
            if self.params["indicator"] in (Indicator.CPV, Indicator.CPM) \
            else 24
        value = item["value"] * 1. / divider
        start_of_day = as_datetime(item["label"])
        return [dict(value=value, label=start_of_day + timedelta(hours=i))
                for i in range(24)]

    def _plan_placement_value_for_date(self, placement, date) -> tuple:
        if placement["start"] > date or placement["end"] < date:
            return (0,)
        indicator = self.params.get("indicator")
        total_days = (placement["end"] - placement["start"]).days + 1
        if indicator in (Indicator.IMPRESSIONS, Indicator.VIEWS):
            return (placement["ordered_units"] / total_days,)
        if indicator == Indicator.COST:
            return (placement["total_cost"] / total_days,)
        if indicator == Indicator.CPV:
            return placement["total_cost"], placement["ordered_units"]
        if indicator == Indicator.CPM:
            return placement["total_cost"], placement["ordered_units"] / 1000.
        return (0,)

    @staticmethod
    def get_camp_link(queryset):
        fields = get_field_names_from_opts(queryset.model._meta)
        camp_link = "campaign"
        if camp_link not in fields:
            if "ad_group" in fields:
                camp_link = "ad_group__campaign"
            elif "ad" in fields:
                camp_link = "ad__ad_group__campaign"
        return camp_link

    def _serialize_items(self, items):
        return [self._serialize_item(item) for item in items]

    def _serialize_item(self, item):
        return {key: value for key, value in item.items()
                if key in self.FIELDS_TO_SERIALIZE}

    def _get_interest_data(self):
        raw_stats = self.get_top_data(
            AudienceStatistic.objects.all(),
            "audience_id",
        )

        ids = set(s["audience_id"] for s in raw_stats)
        labels = {
            c["id"]: c["name"]
            for c in Audience.objects.filter(pk__in=ids).values("id", "name")
        }
        result = defaultdict(list)
        for item in raw_stats:
            audience_id = item["audience_id"]
            label = labels.get(audience_id, audience_id)
            del item["audience_id"]
            result[label].append(item)

        return result

    def _get_remarketing_data(self):
        raw_stats = self.get_top_data(
            RemarkStatistic.objects.all(),
            "remark_id",
        )
        ids = set(s["remark_id"] for s in raw_stats)
        labels = dict(
            RemarkList.objects.filter(pk__in=ids).values_list("id", "name")
        )
        result = defaultdict(list)
        for item in raw_stats:
            item_id = item["remark_id"]
            label = labels.get(item_id, item_id)
            del item["remark_id"]
            result[label].append(item)

        return result

    def get_top_data(self, queryset, key):
        raise NotImplementedError

    def _get_ad_data(self):
        group_by = ["ad__creative_name", "ad_id", "ad__status"]
        raw_stats = self.get_raw_stats(
            AdStatistic.objects.all(), group_by,
            self.params["date"],
        )
        result = defaultdict(list)
        for item in raw_stats:
            uid = item["ad_id"]
            item["label"] = "{} #{}".format(item["ad__creative_name"], uid)
            item["status"] = item["ad__status"]
            del item["ad__creative_name"], item["ad_id"], item["ad__status"]
            result[uid].append(item)
        return result

    def _get_device_data(self):
        group_by = ["device_id"]
        raw_stats = self.get_raw_stats(
            AdGroupStatistic.objects.all(), group_by,
            self.params["date"]
        )
        result = defaultdict(list)
        for item in raw_stats:
            label = device_str(item["device_id"])
            del item["device_id"]
            result[label].append(item)

        return result

    def _get_age_data(self):

        group_by = ["age_range_id"]
        raw_stats = self.get_raw_stats(
            AgeRangeStatistic.objects.all(), group_by,
        )
        result = defaultdict(list)
        for item in raw_stats:
            label = AgeRanges[item["age_range_id"]]
            del item["age_range_id"]
            result[label].append(item)

        return result

    def _get_gender_data(self):
        group_by = ["gender_id"]
        raw_stats = self.get_raw_stats(
            GenderStatistic.objects.all(), group_by,
        )
        result = defaultdict(list)
        for item in raw_stats:
            label = Genders[item["gender_id"]]
            del item["gender_id"]
            result[label].append(item)

        return result

    def _get_topic_data(self):

        stats = self.get_top_data(
            TopicStatistic.objects.all(),
            "topic_id",
        )
        ids = set(s["topic_id"] for s in stats)
        labels = dict(
            Topic.objects.filter(pk__in=ids).values_list("id", "name")
        )

        result = defaultdict(list)
        for item in stats:
            item_id = item["topic_id"]
            label = labels.get(item_id, item_id)
            del item["topic_id"]
            result[label].append(item)

        return result

    def _get_keyword_data(self):

        stats = self.get_top_data(
            KeywordStatistic.objects.all(),
            "keyword",
        )

        result = defaultdict(list)
        for item in stats:
            label = item["keyword"]
            del item["keyword"]
            result[label].append(item)

        return result

    def _get_location_data(self):

        raw_stats = self.get_top_data(
            CityStatistic.objects.all(),
            "city_id",
        )

        ids = set(c["city_id"] for c in raw_stats)
        cities = GeoTarget.objects.only("canonical_name").in_bulk(ids)

        result = defaultdict(list)
        for item in raw_stats:
            city = cities.get(item["city_id"])
            label = city.canonical_name if city else item["city_id"]
            del item["city_id"]
            result[label].append(item)

        return result

    def get_account_segmented_data(self):

        yesterday = now_in_default_tz().date() - timedelta(days=1)
        four_days_ago = yesterday - timedelta(days=4)

        values_func = self.get_values_func()

        if self.params["breakdown"] == Breakdown.HOURLY:
            queryset = CampaignHourlyStatistic.objects.all()
            account_id_field = "campaign__account_id"
            account_name_field = "campaign__account__name"
            order_by = [account_id_field, "date", "hour"]
        else:
            queryset = AdGroupStatistic.objects.all()
            account_id_field = "ad_group__campaign__account_id"
            account_name_field = "ad_group__campaign__account__name"
            order_by = [account_id_field, "date"]

        values = order_by + [account_name_field]

        queryset = self.filter_queryset(queryset)
        queryset = queryset.values(*values).order_by(*order_by)
        raw_data = self.add_annotate(queryset)
        # response
        data = []
        if raw_data:
            current_account = None
            item = None
            sum_1d = count_1d = sum_5d = count_5d = 0

            for s in raw_data:
                account_id = s[account_id_field]
                if account_id != current_account:
                    if item:
                        item.update(
                            average_1d=sum_1d / count_1d if count_1d else None,
                            average_5d=sum_5d / count_5d if count_5d else None,
                        )
                        data.append(item)
                    # next account
                    current_account = account_id
                    item = dict(
                        id=s[account_id_field],
                        label=s[account_name_field],
                        trend=[],
                    )
                    sum_1d = count_1d = sum_5d = count_5d = 0

                value = values_func(s)
                date = s["date"]
                if value and date >= four_days_ago:
                    sum_5d += value
                    count_5d += 1

                    if date >= yesterday:
                        sum_1d += value
                        count_1d += 1

                if "hour" in s:
                    date = s["date"]
                    point_label = datetime(
                        year=date.year, month=date.month,
                        day=date.day, hour=s["hour"],
                    )
                else:
                    point_label = s["date"]

                item["trend"].append(
                    dict(
                        label=point_label,
                        value=value,
                    )
                )
            # pylint: disable=useless-else-on-loop
            else:
                if item:
                    item.update(
                        average_1d=sum_1d / count_1d if count_1d else None,
                        average_5d=sum_5d / count_5d if count_5d else None,
                    )
                    data.append(item)
            # pylint: enable=useless-else-on-loop
        return sorted(data, key=lambda i: i["trend"][-1]["label"],
                      reverse=True)

    def filter_queryset(self, queryset):
        raise NotImplementedError

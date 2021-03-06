from typing import Type

from django.db.models import Case
from django.db.models import F
from django.db.models import FloatField as DBFloatField
from django.db.models import OuterRef
from django.db.models import QuerySet
from django.db.models import Subquery
from django.db.models import Sum
from django.db.models import When
from rest_framework.fields import BooleanField
from rest_framework.fields import CharField
from rest_framework.fields import DateField
from rest_framework.fields import FloatField
from rest_framework.fields import IntegerField
from rest_framework.fields import ReadOnlyField
from rest_framework.fields import SerializerMethodField
from rest_framework.serializers import ModelSerializer

from aw_reporting.models import AdGroupStatistic
from aw_reporting.models import AdStatistic
from aw_reporting.models import AgeRangeStatistic
from aw_reporting.models import Audience
from aw_reporting.models import AudienceStatistic
from aw_reporting.models import GenderStatistic
from aw_reporting.models import KeywordStatistic
from aw_reporting.models import TopicStatistic
from aw_reporting.models import age_range_str
from aw_reporting.models import device_str
from aw_reporting.models import gender_str
from aw_reporting.models import get_ctr
from aw_reporting.models import get_video_view_rate
from aw_reporting.models import goal_type_str
from aw_reporting.models.salesforce_constants import SalesForceGoalType
from es_components.constants import Sections
from es_components.managers import ChannelManager
from es_components.managers import VideoManager
from es_components.managers.base import BaseManager

__all__ = [
    "TargetTableTopicSerializer",
    "TargetTableKeywordSerializer",
    "TargetTableChannelSerializer",
    "TargetTableVideoSerializer",
    "TargetTableAudienceSerializer",
    "DevicesTableSerializer",
    "DemoTableSerializer",
    "DemoAgeRangeTableSerializer",
    "DemoGenderTableSerializer",
    "VideosTableSerializer",
]

INTEREST_STR = {
    Audience.CUSTOM_AFFINITY_TYPE: "Interests - CAA",
    Audience.CUSTOM_INTENT_TYPE: "Interests - Custom Intent",
    Audience.AFFINITY_TYPE: "Interests - Affinity",
    Audience.IN_MARKET_TYPE: "Interests - In market",
}


class GoalTypeField(CharField):
    def to_representation(self, goal_type_id):
        goal_type = goal_type_str(goal_type_id)
        return super(GoalTypeField, self).to_representation(goal_type)


class TransformField(CharField):
    def __init__(self, *args, **kwargs):
        self.transform_function = kwargs.pop("transform_function")
        super(TransformField, self).__init__(*args, **kwargs)

    def to_representation(self, value):
        value = self.transform_function(value)
        return super().to_representation(value)


class AdsFloatField(FloatField):
    def to_representation(self, value):
        value = value / 10 ** 6
        return super().to_representation(value)


class TargetTableSerializer(ModelSerializer):
    name = ReadOnlyField(default="N/A")
    type = ReadOnlyField(default="N/A")
    campaign_name = CharField(source="ad_group__campaign__name")
    ad_group_name = CharField(source="ad_group__name")
    placement_name = CharField(source="ad_group__campaign__salesforce_placement__name")
    placement_start = DateField(source="ad_group__campaign__salesforce_placement__start")
    placement_end = DateField(source="ad_group__campaign__salesforce_placement__end")
    margin_cap = BooleanField(source="ad_group__campaign__salesforce_placement__opportunity__margin_cap_required")
    max_bid = AdsFloatField(source="ad_group__cpv_bid")
    cannot_roll_over = BooleanField(source="ad_group__campaign__salesforce_placement__opportunity__cannot_roll_over")
    rate_type = GoalTypeField(source="ad_group__campaign__salesforce_placement__goal_type_id")
    contracted_rate = FloatField(source="ad_group__campaign__salesforce_placement__ordered_rate")
    impressions = IntegerField(source="sum_impressions")
    video_views = IntegerField(source="sum_video_views")
    clicks = IntegerField(source="sum_clicks")
    cost = FloatField(source="sum_cost")
    ctr = SerializerMethodField()
    view_rate = SerializerMethodField()
    days_remaining = SerializerMethodField()
    avg_rate = SerializerMethodField()
    revenue = SerializerMethodField()
    profit = SerializerMethodField()
    margin = SerializerMethodField()
    video_played_to_100 = SerializerMethodField()
    cost_delivery_percentage = SerializerMethodField()
    delivery_percentage = SerializerMethodField()

    def get_ctr(self, obj):
        return get_ctr(
            impressions=obj["sum_impressions"],
            clicks=obj["sum_clicks"],
        )

    def get_view_rate(self, obj):
        return get_video_view_rate(
            video_impressions=obj["sum_video_impressions"],
            video_views=obj["sum_video_views"],
        )

    def get_days_remaining(self, obj):
        placement_end = obj[f"{self.Meta.ad_group_ref}__campaign__salesforce_placement__end"]
        today = self.context["now"].date()
        try:
            remaining = (placement_end - today).days
        except (ValueError, TypeError, AttributeError):
            remaining = None
        return remaining

    def get_avg_rate(self, obj):
        cost = obj["sum_cost"]
        units = self._get_units(obj)
        divider = self._get_units_divider(obj)
        try:
            return cost / (units / divider)
        except (TypeError, ZeroDivisionError):
            return None

    def get_revenue(self, obj):
        units = self._get_units(obj)
        rate = obj[f"{self.Meta.ad_group_ref}__campaign__salesforce_placement__ordered_rate"]
        divider = self._get_units_divider(obj)
        try:
            revenue = units * rate / divider
        except (TypeError, ZeroDivisionError):
            revenue = None
        return revenue

    def get_profit(self, obj):
        revenue = self.get_revenue(obj)
        cost = obj["sum_cost"]
        try:
            profit = revenue - cost
        except TypeError:
            profit = None
        return profit

    def get_margin(self, obj):
        profit = self.get_profit(obj)
        revenue = self.get_revenue(obj)
        try:
            margin = profit / revenue
        except (TypeError, ZeroDivisionError):
            margin = None
        return margin

    def get_video_played_to_100(self, obj):
        try:
            val = obj["sum_video_views_100_quartile"] / obj["sum_impressions"]
        except (TypeError, ZeroDivisionError):
            val = None
        return val

    def get_cost_delivery_percentage(self, obj):
        try:
            val = obj["sum_cost"] / obj["sum_type_cost"]
        except (TypeError, ZeroDivisionError):
            val = None
        return val

    def get_delivery_percentage(self, obj):
        delivered = self._get_units(obj)
        sum_delivery = obj["sum_type_delivery"]
        try:
            val = delivered / sum_delivery
        except (TypeError, ZeroDivisionError):
            val = None
        return val

    def _get_units(self, obj):
        goal_type_id = self._get_goal_type_id(obj)
        units = 0
        if goal_type_id == SalesForceGoalType.CPV:
            units = obj["sum_video_views"]
        elif goal_type_id == SalesForceGoalType.CPM:
            units = obj["sum_impressions"]
        return units

    def _get_units_divider(self, obj):
        goal_type_id = self._get_goal_type_id(obj)
        return (1000
                if goal_type_id == SalesForceGoalType.CPM
                else 1)

    def _get_goal_type_id(self, obj):
        return obj[f"{self.Meta.ad_group_ref}__campaign__salesforce_placement__goal_type_id"]

    def __new__(cls, *args, **kwargs):
        if args and isinstance(args[0], QuerySet):
            queryset = cls._build_queryset(args[0])
            args = (queryset,) + args[1:]
        return super().__new__(cls, *args, **kwargs)

    @classmethod
    def _build_type_subquery(cls, queryset):
        return queryset.filter(ad_group_id=OuterRef("ad_group_id")) \
            .order_by("ad_group_id") \
            .values("ad_group_id")

    @classmethod
    def _build_queryset(cls, queryset):
        type_subquery = cls._build_type_subquery(queryset)
        subquery_cost = type_subquery.annotate(sum=Sum("cost")).values("sum")
        goal_type_ref = f"{cls.Meta.ad_group_ref}__campaign__salesforce_placement__goal_type_id"
        subquery_delivery = type_subquery.annotate(sum=Sum(Case(
            When(
                **{goal_type_ref: SalesForceGoalType.CPV},
                then=F("video_views")
            ),
            When(
                **{goal_type_ref: SalesForceGoalType.CPM},
                then=F("impressions")
            )
        ))).values("sum")
        return queryset.values(*cls.Meta.group_by, *cls.Meta.values_shared) \
            .annotate(sum_impressions=Sum("impressions"),
                      sum_video_views=Sum("video_views"),
                      sum_clicks=Sum("clicks"),
                      sum_cost=Sum("cost"),
                      sum_video_impressions=Sum(Case(When(
                          **{goal_type_ref: SalesForceGoalType.CPV},
                          then=F("impressions")
                      ))),
                      sum_video_views_100_quartile=Sum("video_views_100_quartile"),
                      sum_type_cost=Subquery(subquery_cost, output_field=DBFloatField()),
                      sum_type_delivery=Subquery(subquery_delivery, output_field=DBFloatField()), ) \
            .order_by("-sum_video_views")

    class Meta:
        model = None
        fields = (
            "name",
            "type",
            "campaign_name",
            "ad_group_name",
            "placement_name",
            "placement_start",
            "placement_end",
            "margin_cap",
            "max_bid",
            "cannot_roll_over",
            "rate_type",
            "contracted_rate",
            "impressions",
            "video_views",
            "clicks",
            "cost",
            "ctr",
            "view_rate",
            "days_remaining",
            "avg_rate",
            "revenue",
            "profit",
            "margin",
            "video_played_to_100",
            "cost_delivery_percentage",
            "delivery_percentage",
        )
        group_by = ("id",)
        values_shared = (
            "ad_group__campaign__name",
            "ad_group__name",
            "ad_group__cpv_bid",
            "ad_group__campaign__salesforce_placement__name",
            "ad_group__campaign__salesforce_placement__start",
            "ad_group__campaign__salesforce_placement__end",
            "ad_group__campaign__salesforce_placement__opportunity__cannot_roll_over",
            "ad_group__campaign__salesforce_placement__opportunity__margin_cap_required",
            "ad_group__campaign__salesforce_placement__goal_type_id",
            "ad_group__campaign__salesforce_placement__ordered_rate",
        )
        ad_group_ref = "ad_group"


class TargetTableTopicSerializer(TargetTableSerializer):
    name = CharField(source="topic__name")
    type = ReadOnlyField(default="Topic")

    class Meta(TargetTableSerializer.Meta):
        model = TopicStatistic
        group_by = ("topic_id", "topic__name")


class TargetTableKeywordSerializer(TargetTableSerializer):
    name = CharField(source="keyword")
    type = ReadOnlyField(default="Keyword")

    class Meta(TargetTableSerializer.Meta):
        model = KeywordStatistic
        group_by = ("keyword",)


class ESTitleField(CharField):
    def __init__(self, *args, **kwargs):
        es_manager_cls: Type[BaseManager] = kwargs.pop("es_manager_cls")
        self.es_manager = es_manager_cls(Sections.GENERAL_DATA)
        super(ESTitleField, self).__init__(*args, **kwargs)

    def to_representation(self, yt_id):
        item = self.es_manager.get([yt_id])[0]
        value = item.general_data.title if item and item.general_data else yt_id
        return super().to_representation(value)


class TargetTableChannelSerializer(TargetTableSerializer):
    name = ESTitleField(source="yt_id", es_manager_cls=ChannelManager)
    type = ReadOnlyField(default="Channel")

    class Meta(TargetTableSerializer.Meta):
        model = KeywordStatistic
        group_by = ("yt_id",)


class TargetTableVideoSerializer(TargetTableSerializer):
    name = ESTitleField(source="yt_id", es_manager_cls=VideoManager)
    type = ReadOnlyField(default="Video")

    class Meta(TargetTableSerializer.Meta):
        model = KeywordStatistic
        group_by = ("yt_id",)


class TargetTableAudienceSerializer(TargetTableSerializer):
    type = TransformField(source="audience__type", transform_function=lambda value: INTEREST_STR.get(value, value))
    name = CharField(source="audience__name")

    class Meta(TargetTableSerializer.Meta):
        model = AudienceStatistic
        group_by = ("audience_id", "audience__name", "audience__type")

    @classmethod
    def _build_type_subquery(cls, queryset):
        return queryset.filter(ad_group_id=OuterRef("ad_group_id"), audience__type=OuterRef("audience__type")) \
            .order_by("ad_group_id", "audience__type") \
            .values("ad_group_id", "audience__type")


class DevicesTableSerializer(TargetTableSerializer):
    type = TransformField(source="device_id", transform_function=device_str)

    class Meta(TargetTableSerializer.Meta):
        model = AdGroupStatistic
        group_by = ("device_id",)

    @classmethod
    def _build_type_subquery(cls, queryset):
        return queryset.filter(ad_group__campaign_id=OuterRef("ad_group__campaign_id")) \
            .order_by("ad_group__campaign_id") \
            .values("ad_group__campaign_id")


class DemoTableSerializer(TargetTableSerializer):
    @classmethod
    def _build_type_subquery(cls, queryset):
        return queryset.filter(ad_group__campaign_id=OuterRef("ad_group__campaign_id")) \
            .order_by("ad_group__campaign_id") \
            .values("ad_group__campaign_id")


class DemoAgeRangeTableSerializer(DemoTableSerializer):
    name = TransformField(source="age_range_id", transform_function=age_range_str)

    class Meta(DemoTableSerializer.Meta):
        model = AgeRangeStatistic
        group_by = ("age_range_id",)


class DemoGenderTableSerializer(DemoTableSerializer):
    name = TransformField(source="gender_id", transform_function=gender_str)

    class Meta(DemoTableSerializer.Meta):
        model = GenderStatistic
        group_by = ("gender_id",)


class VideosTableSerializer(TargetTableSerializer):
    name = CharField(source="ad__creative_name")
    campaign_name = CharField(source="ad__ad_group__campaign__name")
    ad_group_name = CharField(source="ad__ad_group__name")
    placement_name = CharField(source="ad__ad_group__campaign__salesforce_placement__name")
    placement_start = DateField(source="ad__ad_group__campaign__salesforce_placement__start")
    placement_end = DateField(source="ad__ad_group__campaign__salesforce_placement__end")
    max_bid = AdsFloatField(source="ad__ad_group__cpv_bid")
    cannot_roll_over = BooleanField(source="ad__ad_group__campaign__salesforce_placement"
                                           "__opportunity__cannot_roll_over")
    rate_type = GoalTypeField(source="ad__ad_group__campaign__salesforce_placement__goal_type_id")
    contracted_rate = FloatField(source="ad__ad_group__campaign__salesforce_placement__ordered_rate")
    margin_cap = BooleanField(source="ad__ad_group__campaign__salesforce_placement__opportunity__margin_cap_required")

    @classmethod
    def _build_type_subquery(cls, queryset):
        return queryset.filter(ad__ad_group__campaign_id=OuterRef("ad__ad_group__campaign_id")) \
            .order_by("ad__ad_group__campaign_id") \
            .values("ad__ad_group__campaign_id")

    @classmethod
    def _build_queryset(cls, queryset):
        queryset = queryset.annotate(ad_group=F("ad__ad_group"))
        return super()._build_queryset(queryset)

    class Meta(TargetTableSerializer.Meta):
        model = AdStatistic
        group_by = ("ad_id", "ad__creative_name")
        values_shared = tuple([f"ad__{value}" for value in TargetTableSerializer.Meta.values_shared])
        ad_group_ref = "ad__ad_group"

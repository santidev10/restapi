from django.db.models import Case
from django.db.models import F
from django.db.models import QuerySet
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

from aw_reporting.models import KeywordStatistic
from aw_reporting.models import TopicStatistic
from aw_reporting.models import get_ctr
from aw_reporting.models import get_video_view_rate
from aw_reporting.models import goal_type_str
from aw_reporting.models.salesforce_constants import SalesForceGoalType

__all__ = [
    "TargetTableTopicSerializer",
    "TargetTableKeywordSerializer",
]


class GoalTypeField(CharField):
    def to_representation(self, goal_type_id):
        goal_type = goal_type_str(goal_type_id)
        return super(GoalTypeField, self).to_representation(goal_type)


class TargetTableSerializer(ModelSerializer):
    name = ReadOnlyField(default="N/A")
    type = ReadOnlyField(default="N/A")
    campaign_name = CharField(source="ad_group__campaign__name")
    ad_group_name = CharField(source="ad_group__name")
    placement_name = CharField(source="ad_group__campaign__salesforce_placement__name")
    placement_start = DateField(source="ad_group__campaign__salesforce_placement__start")
    placement_end = DateField(source="ad_group__campaign__salesforce_placement__end")
    margin_cap = ReadOnlyField(default="N/A")
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
        placement_end = obj["ad_group__campaign__salesforce_placement__end"]
        today = self.context["now"].date()
        return (placement_end - today).days

    def get_avg_rate(self, obj):
        cost = obj["sum_cost"]
        units = self._get_units(obj)
        divider = self._get_units_divider(obj)
        try:
            return cost / (units / divider)
        except ZeroDivisionError:
            return None

    def get_revenue(self, obj):
        units = self._get_units(obj)
        rate = obj["ad_group__campaign__salesforce_placement__ordered_rate"]
        divider = self._get_units_divider(obj)
        return units * rate / divider

    def get_profit(self, obj):
        revenue = self.get_revenue(obj)
        cost = obj["sum_cost"]
        return revenue - cost

    def get_margin(self, obj):
        profit = self.get_profit(obj)
        revenue = self.get_revenue(obj)
        return (profit / revenue
                if revenue
                else None)

    def get_video_played_to_100(self, obj):
        impressions = obj["sum_impressions"]
        if impressions:
            return obj["sum_video_views_100_quartile"] / obj["sum_impressions"]
        return None

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
        return obj["ad_group__campaign__salesforce_placement__goal_type_id"]

    def __new__(cls, *args, **kwargs):
        if args and isinstance(args[0], QuerySet):
            queryset = cls._build_queryset(args[0])
            args = (queryset,) + args[1:]
        return super().__new__(cls, *args, **kwargs)

    @classmethod
    def _build_queryset(cls, queryset):
        return queryset.values(*cls.Meta.group_by, *cls.Meta.values_shared) \
            .order_by(*cls.Meta.group_by) \
            .annotate(sum_impressions=Sum("impressions"),
                      sum_video_views=Sum("video_views"),
                      sum_clicks=Sum("clicks"),
                      sum_cost=Sum("cost"),
                      sum_video_impressions=Sum(Case(When(
                          ad_group__campaign__salesforce_placement__goal_type_id=SalesForceGoalType.CPV,
                          then=F("impressions")
                      ))),
                      sum_video_views_100_quartile=Sum("video_views_100_quartile"), )

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
        )
        group_by = ("id",)
        values_shared = (
            "ad_group__campaign__name",
            "ad_group__name",
            "ad_group__campaign__salesforce_placement__name",
            "ad_group__campaign__salesforce_placement__start",
            "ad_group__campaign__salesforce_placement__end",
            "ad_group__campaign__salesforce_placement__opportunity__cannot_roll_over",
            "ad_group__campaign__salesforce_placement__opportunity__cannot_roll_over",
            "ad_group__campaign__salesforce_placement__goal_type_id",
            "ad_group__campaign__salesforce_placement__ordered_rate",
        )


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

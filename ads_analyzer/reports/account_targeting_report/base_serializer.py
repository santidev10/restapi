from django.db.models import Case
from django.db.models import F
from django.db.models import ExpressionWrapper
from django.db.models import FloatField as DBFloatField
from django.db.models import QuerySet
from django.db.models import Sum
from django.db.models import When
from django.db.models.functions.comparison import NullIf
from rest_framework.fields import CharField
from rest_framework.fields import FloatField
from rest_framework.fields import IntegerField
from rest_framework.fields import ReadOnlyField
from rest_framework.serializers import ModelSerializer

from aw_reporting.models.salesforce_constants import SalesForceGoalType


class BaseSerializer(ModelSerializer):
    """
    Serializer base class for AccountTargetingReport statistics models
    """
    # Values should be set by children
    criterion_name = ReadOnlyField(default="N/A")
    target_name = ReadOnlyField(default="N/A")
    type = ReadOnlyField(default="N/A")

    # cls.Meta.values_shared
    ad_group_name = CharField(source="ad_group__name")
    ad_group_id = IntegerField(source="ad_group__id")
    campaign_name = CharField(source="ad_group__campaign__name")
    campaign_id = IntegerField(source="ad_group__campaign__id")
    rate_type = IntegerField(source="ad_group__campaign__salesforce_placement__goal_type_id")
    contracted_rate = FloatField(source="ad_group__campaign__salesforce_placement__ordered_rate")

    # Added in second annotation of _build_queryset
    impressions = IntegerField(source="sum_impressions")
    video_views = IntegerField(source="sum_video_views")
    clicks = IntegerField(source="sum_clicks")
    cost = FloatField(source="sum_cost")

    # Added during last annotation of _build_queryset
    average_cpm = FloatField()
    average_cpv = FloatField()
    cost_share = FloatField()
    ctr_i = FloatField()
    ctr_v = FloatField()
    impressions_share = FloatField()
    margin = FloatField()
    profit = FloatField()
    video_views_share = FloatField()
    view_rate = FloatField()

    class Meta:
        model = None
        fields = (
            "target_name",
            "type",
            "campaign_id",
            "campaign_name",
            "ad_group_id",
            "ad_group_name",
            "impressions",
            "contracted_rate",
            "impressions_share",
            "video_views_share",
            "cost_share",
            "average_cpm",
            "average_cpv",
            "video_views",
            "clicks",
            "cost",
            "ctr_i",
            "ctr_v",
            "view_rate",
            "profit",
            "margin",
        )
        group_by = ("id",)
        values_shared = (
            "ad_group__impressions",
            "ad_group__cost",
            "ad_group__video_views",
            "ad_group__id",
            "ad_group__campaign__name",
            "ad_group__campaign__id",
            "ad_group__name",
            "ad_group__cpv_bid",
            "ad_group__campaign__salesforce_placement__goal_type_id",
            "ad_group__campaign__salesforce_placement__ordered_rate",
        )
        ad_group_ref = "ad_group"

    def __new__(cls, *args, **kwargs):
        aggregated = None
        if args and isinstance(args[0], QuerySet):
            params = kwargs.get("context", {}).get("params")
            aggregated = queryset = cls._build_queryset(args[0], params=params)
            args = (queryset,) + args[1:]
        instance = super().__new__(cls, *args, **kwargs)
        instance.aggregated_queryset = aggregated
        return instance

    @classmethod
    def _build_queryset(cls, queryset, params=None):
        """
        Construct queryset
        Assign values for aggregation of original queryset statistics, and apply additional
            annotations for calculated kpi values
        :param queryset: Queryset
        :param params: dict[filters, ...]
        :return:
        """
        params = params or {}
        goal_type_ref = "ad_group__campaign__salesforce_placement__goal_type_id"
        queryset = queryset \
            .values(*cls.Meta.group_by, *cls.Meta.values_shared) \
            .annotate(
                contracted_rate=F("ad_group__campaign__salesforce_placement__ordered_rate"),
                sum_impressions=Sum("impressions"),
                sum_video_views=Sum("video_views"),
                sum_clicks=Sum("clicks"),
                sum_cost=Sum("cost"),
                sum_video_views_100_quartile=Sum("video_views_100_quartile"),
                sum_video_impressions=Sum(Case(When(
                    **{goal_type_ref: SalesForceGoalType.CPV},
                    then=F("impressions")
                ))),
                revenue=Case(
                    When(
                        ad_group__campaign__salesforce_placement__goal_type_id=0,
                        then=F("sum_impressions") * F("ad_group__campaign__salesforce_placement__ordered_rate") / 1000,
                    ),
                    default=F("sum_video_views") * F("ad_group__campaign__salesforce_placement__ordered_rate"),
                    output_field=DBFloatField()
                )
            ) \
            .annotate(
                view_rate=ExpressionWrapper(
                    F("sum_video_views") * 1.0 / NullIf(F("sum_video_impressions"), 0),
                    output_field=DBFloatField(),
                ),
                impressions_share=ExpressionWrapper(
                    F("sum_impressions") * 1.0 / NullIf(F("ad_group__impressions"), 0),
                    output_field=DBFloatField(),
                ),
                video_views_share=ExpressionWrapper(
                    F("sum_video_views") * 1.0 / NullIf(F("ad_group__video_views"), 0),
                    output_field=DBFloatField(),
                ),
                cost_share=ExpressionWrapper(
                    F("sum_cost") * 1.0 / NullIf(F("ad_group__cost"), 0),
                    output_field=DBFloatField(),
                ),
                ctr_i=ExpressionWrapper(
                    F("sum_clicks") * 1.0 / NullIf(F("sum_impressions"), 0),
                    output_field=DBFloatField(),
                ),
                ctr_v=ExpressionWrapper(
                    F("sum_clicks") * 1.0 / NullIf(F("sum_video_views"), 0),
                    output_field=DBFloatField(),
                ),
                average_cpm=ExpressionWrapper(
                    F("sum_cost") * 1.0 / NullIf(F("sum_impressions"), 0) * 1000,
                    output_field=DBFloatField(),
                ),
                average_cpv=ExpressionWrapper(
                    F("sum_cost") * 1.0 / NullIf(F("sum_video_views"), 0),
                    output_field=DBFloatField(),
                ),
                profit=F("revenue") - F("sum_cost"),
                margin=(F("revenue") - F("sum_cost")) / NullIf(F("revenue"), 0)
            )
        queryset = cls._filter_queryset(queryset, params.get("filters"))
        queryset.order_by()
        return queryset

    @classmethod
    def _filter_queryset(cls, queryset, filters):
        """
        Apply filters to aggregated statistics queryset
        :param queryset: Queryset
        :param filters: dict
        :return:
        """
        if filters is not None:
            queryset = queryset.filter(**filters)
        return queryset

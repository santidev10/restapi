from django.db.models import QuerySet
from rest_framework.fields import CharField
from rest_framework.fields import FloatField
from rest_framework.fields import IntegerField
from rest_framework.fields import ReadOnlyField
from rest_framework.serializers import ModelSerializer

from .constants import TargetingStatisticsConfigs
from ads_analyzer.reports.account_targeting_report.annotations import ANNOTATIONS


class BaseSerializer(ModelSerializer):
    """
    Serializer base class for AccountTargetingReport statistics models
    """
    # Values should be set by children
    config = None
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
    video_view_rate = FloatField()

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
            "video_view_rate",
            "profit",
            "margin",
        )
        group_by = ("id",)
        values_shared = (
            "ad_group__campaign__name",
            "ad_group__campaign__id",
            "ad_group__campaign__salesforce_placement__goal_type_id",
            "ad_group__campaign__salesforce_placement__ordered_rate",
        )
        ad_group_ref = "ad_group"

    def __new__(cls, *args, **kwargs):
        aggregated = None
        if args and isinstance(args[0], QuerySet):
            kpi_filters = kwargs["context"].get("kpi_filters")
            aggregation_keys = kwargs["context"]["aggregation_keys"]
            all_targeting = kwargs["context"].get("all_targeting", False)
            # if all_targeting is False:
            cls.Meta.group_by += ("ad_group__id",)
            cls.Meta.values_shared += ("ad_group__impressions", "ad_group__cost", "ad_group__video_views",
                                       "ad_group__cpv_bid", "ad_group__id", "ad_group__name",)
            aggregated = queryset = cls._build_queryset(args[0], aggregation_keys, kpi_filters=kpi_filters)
            args = (queryset,) + args[1:]
        instance = super().__new__(cls, *args, **kwargs)
        instance.aggregated_queryset = aggregated
        return instance

    @classmethod
    def _build_queryset(cls, queryset, aggregation_keys, kpi_filters=None):
        """
        Construct queryset
        Assign values for aggregation of original queryset statistics, and apply additional
            annotations for calculated kpi values
        :param queryset: Queryset
        :param params: dict[filters, ...]
        :return:
        """
        kpi_filters = kpi_filters or {}
        statistics_annotations = {column: ANNOTATIONS[column]
                                  for column in TargetingStatisticsConfigs.STATISTICS_ANNOTATIONS}
        aggregate_annotations = {column: ANNOTATIONS[column] for column in aggregation_keys}
        queryset = queryset \
            .values(*cls.Meta.group_by, *cls.Meta.values_shared) \
            .annotate(
                **statistics_annotations,
            ) \
            .annotate(
                **aggregate_annotations
            ).order_by()
        queryset = cls._filter_queryset(queryset, kpi_filters)
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
            queryset = queryset.filter(**filters).order_by()
        return queryset

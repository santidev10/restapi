import hashlib

from django.db.models import Case
from django.db.models import CharField as DBCharField
from django.db.models import F
from django.db.models import FloatField as DBFloatField
from django.db.models import OuterRef
from django.db.models import QuerySet
from django.db.models import Subquery
from django.db.models import Value
from django.db.models import When
from django.db.models.functions import Cast
from rest_framework.fields import CharField
from rest_framework.fields import FloatField
from rest_framework.fields import IntegerField
from rest_framework.serializers import ModelSerializer
from rest_framework.serializers import SerializerMethodField

from .constants import STATISTICS_ANNOTATIONS
from .constants import IMPRESSIONS_SHARE
from .constants import VIDEO_VIEWS_SHARE
from ads_analyzer.reports.account_targeting_report.annotations import ANNOTATIONS
from aw_reporting.models import AdGroupTargeting
from aw_reporting.models import TargetingStatusEnum


class BaseSerializer(ModelSerializer):
    """
    Serializer base class for AccountTargetingReport statistics models
    """
    targeting_id = SerializerMethodField()

    # Values should be set by children
    report_name = None
    criteria_field = None
    type_id = None
    config = None
    criteria = None
    type = None
    target_name = None
    type_name = None

    # cls.Meta.values_shared
    ad_group_id = IntegerField()
    ad_group_name = CharField(source="ad_group__name")
    campaign_name = CharField(source="ad_group__campaign__name")
    campaign_status = CharField(source="ad_group__campaign__status")
    campaign_id = IntegerField(source="ad_group__campaign__id")
    rate_type = IntegerField(source="ad_group__campaign__salesforce_placement__goal_type_id")
    contracted_rate = FloatField(source="ad_group__campaign__salesforce_placement__ordered_rate")

    # Added in second annotation of _build_queryset
    sum_impressions = IntegerField()
    sum_video_views = IntegerField()
    sum_clicks = IntegerField()
    sum_cost = FloatField()

    targeting_status = SerializerMethodField()

    # Added during last annotation of _build_queryset
    revenue = FloatField()
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
            "targeting_id",
            "target_name",
            "type",
            "type_name",
            "campaign_id",
            "campaign_name",
            "campaign_status",
            "criteria",
            "ad_group_id",
            "ad_group_name",
            "contracted_rate",
            "impressions_share",
            "video_views_share",
            "cost_share",
            "average_cpm",
            "average_cpv",
            "ctr_i",
            "ctr_v",
            "revenue",
            "rate_type",
            "video_view_rate",
            "profit",
            "margin",
            "sum_impressions",
            "sum_video_views",
            "sum_clicks",
            "sum_cost",
            "targeting_status",
        )
        group_by = ("id",)
        values_shared = (
            "ad_group__impressions",
            "ad_group__cost",
            "ad_group__video_views",
            "ad_group__cpv_bid",
            "ad_group_id",
            "ad_group__name",
            "ad_group__campaign__name",
            "ad_group__campaign__status",
            "ad_group__campaign__id",
            "ad_group__campaign__impressions",
            "ad_group__campaign__video_views",
            "ad_group__campaign__cost",
            "ad_group__campaign__salesforce_placement__goal_type_id",
            "ad_group__campaign__salesforce_placement__ordered_rate",
        )
        ad_group_ref = "ad_group"

    def __new__(cls, *args, **kwargs):
        aggregated = None
        if args and isinstance(args[0], QuerySet):
            kpi_filters = kwargs["context"].get("kpi_filters")
            aggregation_keys = kwargs["context"]["aggregation_keys"]
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
        statistics_annotations = {column: ANNOTATIONS[column] for column in STATISTICS_ANNOTATIONS}
        aggregate_annotations = {column: ANNOTATIONS[column] for column in aggregation_keys}
        queryset = queryset \
            .values(*cls.Meta.group_by, *cls.Meta.values_shared) \
            .annotate(
                **statistics_annotations,
            ) \
            .annotate(
                **aggregate_annotations
            ).order_by()
        # Add targeting status if serializer has targeting
        if cls.criteria_field:
            targeting_subquery = AdGroupTargeting.objects.filter(
                ad_group_id=OuterRef("ad_group_id"),
                type_id=cls.type_id,
                statistic_criteria=Cast(OuterRef(cls.criteria_field), output_field=DBCharField()),
            )
            queryset = queryset.annotate(
                targeting_status=Subquery(targeting_subquery.values("status")[:1]),
            )
        queryset = cls._clean_annotations(queryset, kpi_filters)
        queryset = cls._filter_aggregated(queryset, kpi_filters)
        return queryset

    @classmethod
    def _clean_annotations(cls, queryset, kpi_filters):
        clean_annotations = {}
        if IMPRESSIONS_SHARE in kpi_filters:
            clean_annotations[IMPRESSIONS_SHARE] = Case(
                When(f"{IMPRESSIONS_SHARE}__gt=1.0", then=Value('1.0')),
                default=F(IMPRESSIONS_SHARE),
                output_field=DBFloatField()
            )
        if VIDEO_VIEWS_SHARE in kpi_filters:
            clean_annotations[VIDEO_VIEWS_SHARE] = Case(
                When(f"{VIDEO_VIEWS_SHARE}__gt=1.0", then=Value('1.0')),
                default=F(VIDEO_VIEWS_SHARE),
                output_field=DBFloatField()
            )
        queryset = queryset.annotate(**clean_annotations)
        return queryset

    @classmethod
    def _filter_aggregated(cls, queryset, filters):
        """
        Apply filters to aggregated statistics queryset
        :param queryset: Queryset
        :param filters: dict
        :return:
        """
        if filters is not None:
            queryset = queryset.filter(**filters).order_by()
        return queryset

    def get_targeting_status(self, obj):
        status = obj.get("targeting_status")
        try:
            status_value = TargetingStatusEnum(int(status)).name
        except (ValueError, TypeError):
            status_value = None
        return status_value

    @classmethod
    def get_targeting_id(cls, obj):
        base = f"{cls.report_name}{obj['ad_group__campaign__name']}{obj['ad_group__name']}{obj[cls.criteria_field]}"
        hash_str = hashlib.sha1(str.encode(base)).hexdigest()
        return hash_str

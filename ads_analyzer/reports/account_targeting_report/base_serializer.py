from django.db.models import Subquery
from rest_framework.serializers import CharField
from rest_framework.serializers import IntegerField
from rest_framework.serializers import ReadOnlyField

from django.db.models import Case
from django.db.models import F
from django.db.models import Q
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

from aw_reporting.models import VideoCreativeStatistic
from aw_reporting.models import YTChannelStatistic
from aw_reporting.models import YTVideoStatistic
from aw_reporting.models import AdGroup
from ads_analyzer.reports.opportunity_targeting_report.serializers import TargetTableSerializer
from ads_analyzer.reports.opportunity_targeting_report.serializers import TargetTableTopicSerializer
from ads_analyzer.reports.opportunity_targeting_report.serializers import TargetTableKeywordSerializer
from ads_analyzer.reports.opportunity_targeting_report.serializers import DemoGenderTableSerializer
from ads_analyzer.reports.opportunity_targeting_report.serializers import TargetTableAudienceSerializer
from ads_analyzer.reports.opportunity_targeting_report.serializers import DemoAgeRangeTableSerializer
from ads_analyzer.reports.opportunity_targeting_report.serializers import DevicesTableSerializer
from aw_reporting.models import TopicStatistic
from aw_reporting.models.salesforce_constants import SalesForceGoalType
from aw_reporting.models import get_ctr
from aw_reporting.models import get_ctr_v
from utils.db.get_exists import get_exists
from ads_analyzer.reports.account_targeting_report.constants import BASE_SERIALIZER_FIELDS
from ads_analyzer.reports.opportunity_targeting_report.serializers import TargetTableSerializer
from aw_reporting.models import CriterionType

CRITERION_ID_MAPPING = CriterionType.get_mapping_to_id()


class BaseSerializer(TargetTableSerializer):
    """
    Serializer base class for AccountTargeting
    Inherits from TargetTableSerializer as many field / values are shared
    """
    criterion_name = None
    criterion_id = SerializerMethodField()
    ctr_i = SerializerMethodField()
    ctr_v = SerializerMethodField()

    average_cpm = SerializerMethodField()
    average_cpv = SerializerMethodField()

    campaign_id = IntegerField(source="ad_group__campaign__id")
    ad_group_id = IntegerField(source="ad_group__id")
    rate_type = IntegerField(source="ad_group__campaign__salesforce_placement__goal_type_id")

    sum_type_delivery = FloatField(source="sum_type_cost")
    sum_type_cost = FloatField(source="sum_type_delivery")

    cost_share = SerializerMethodField()
    impressions_share = SerializerMethodField()
    video_views_share = SerializerMethodField()

    class Meta(TargetTableSerializer.Meta):
        fields = (
            "name",
            "type",
            "campaign_id",
            "campaign_name",
            "criterion_id",
            "ad_group_id",
            "ad_group_name",
            "max_bid",
            "rate_type",
            "contracted_rate",
            "impressions",

            "impressions_share",
            "video_views_share",
            "cost_share",

            "video_views",
            "clicks",
            "cost",
            "ctr_i",
            "ctr_v",
            "view_rate",
            "average_cpm",
            "average_cpv",
            "revenue",
            "profit",
            "margin",
        )
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

    def get_criterion_id(self, *_, **__):
        criterion = CRITERION_ID_MAPPING[self.criterion_name]
        return criterion

    def get_ctr_i(self, obj):
        try:
            return get_ctr(impressions=obj["sum_impressions"], clicks=obj["sum_clicks"])
        except (TypeError, ZeroDivisionError):
            value = None
        return value

    def get_ctr_v(self, obj):
        try:
            value = get_ctr_v(impressions=obj["sum_video_views"], clicks=obj["sum_clicks"])
        except (TypeError, ZeroDivisionError):
            value = None
        return value

    def get_average_cpm(self, obj):
        try:
            value = obj["sum_cost"] / (obj["sum_impressions"] / 1000)
        except (TypeError, ZeroDivisionError):
            value = None
        return value

    def get_average_cpv(self, obj):
        try:
            value = obj["sum_cost"] / (obj["sum_video_views"])
        except (TypeError, ZeroDivisionError):
            value = None
        return value

    def get_cost_share(self, obj):
        try:
            cost_share = obj["sum_cost"] / obj["ad_group__cost"]
        except (KeyError, ZeroDivisionError):
            cost_share = None
        return cost_share

    def get_video_views_share(self, obj):
        try:
            views_share = obj["sum_video_views"] / obj["ad_group__video_views"]
        except (KeyError, ZeroDivisionError):
            views_share = None
        return views_share

    def get_impressions_share(self, obj):
        try:
            impressions_share = obj["sum_impressions"] / obj["ad_group__impressions"]
        except (KeyError, ZeroDivisionError):
            impressions_share = None
        return impressions_share

    @classmethod
    def _build_queryset(cls, queryset):
        """
        Overrides TargetTableSerializer class method
        Annotate without unneeded fields used in TargetTableSerializer
        :param queryset:
        :return:
        """
        goal_type_ref = "ad_group__campaign__salesforce_placement__goal_type_id"
        queryset = queryset \
            .values(*cls.Meta.group_by, *cls.Meta.values_shared) \
            .annotate(
                sum_impressions=Sum("impressions"),
                sum_video_views=Sum("video_views"),
                sum_clicks=Sum("clicks"),
                sum_cost=Sum("cost"),
                sum_video_views_100_quartile=Sum("video_views_100_quartile"),
                sum_video_impressions=Sum(Case(When(
                    **{goal_type_ref: SalesForceGoalType.CPV},
                    then=F("impressions")
                ))),
            )
        queryset.query.clear_ordering(force_empty=True)
        return queryset

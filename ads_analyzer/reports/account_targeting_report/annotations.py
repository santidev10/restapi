from django.db.models import Case
from django.db.models import F
from django.db.models import ExpressionWrapper
from django.db.models import FloatField as DBFloatField
from django.db.models import Avg
from django.db.models import Sum
from django.db.models import When
from django.db.models.functions.comparison import NullIf

import ads_analyzer.reports.account_targeting_report.constants as names
from aw_reporting.models.salesforce_constants import SalesForceGoalType


goal_type_ref = "ad_group__campaign__salesforce_placement__goal_type_id"
ANNOTATIONS = {
    names.AVERAGE_CONTRACTED_RATE: Avg("contracted_rate"),
    names.AVERAGE_VIEW_RATE: Avg("video_view_rate"),
    names.SUM_REVENUE: Sum("revenue"),
    names.SUM_PROFIT: Sum("profit"),
    names.SUM_MARGIN: Sum("margin"),
    names.AVERAGE_CTR_I: Avg("ctr_i"),
    names.AVERAGE_CTR_V: Avg("ctr_v"),
    names.AVERAGE_REVENUE: Avg("revenue"),
    names.AVERAGE_COST: Avg("cost"),
    names.CONTRACTED_RATE: F("ad_group__campaign__salesforce_placement__ordered_rate"),
    names.SUM_IMPRESSIONS: Sum("impressions"),
    names.SUM_VIDEO_VIEWS: Sum("video_views"),
    names.SUM_CLICKS: Sum("clicks"),
    names.SUM_COST: Sum("cost"),
    names.SUM_VIDEO_VIEWS_100_QUARTILE: Sum("video_views_100_quartile"),
    names.SUM_VIDEO_IMPRESSIONS: Sum(Case(When(
        **{goal_type_ref: SalesForceGoalType.CPV},
        then=F("impressions")
    ))),
    names.REVENUE: Case(
        When(
            ad_group__campaign__salesforce_placement__goal_type_id=0,
            then=F("sum_impressions") * F("ad_group__campaign__salesforce_placement__ordered_rate") / 1000,
        ),
        default=F("sum_video_views") * F("ad_group__campaign__salesforce_placement__ordered_rate"),
        output_field=DBFloatField()
    ),
    names.VIDEO_VIEW_RATE: ExpressionWrapper(
        F("sum_video_views") * 1.0 / NullIf(F("sum_video_impressions"), 0),
        output_field=DBFloatField()
    ),
    names.IMPRESSIONS_SHARE: ExpressionWrapper(
        F("sum_impressions") * 1.0 / NullIf(F("ad_group__impressions"), 0),
        output_field=DBFloatField()
    ),
    names.VIDEO_VIEWS_SHARE: ExpressionWrapper(
        F("sum_video_views") * 1.0 / NullIf(F("ad_group__video_views"), 0),
        output_field=DBFloatField()
    ),
    names.COST_SHARE: ExpressionWrapper(
        F("sum_cost") * 1.0 / NullIf(F("ad_group__cost"), 0),
        output_field=DBFloatField()
    ),
    names.CTR_I: ExpressionWrapper(
        F("sum_clicks") * 1.0 / NullIf(F("sum_impressions"), 0),
        output_field=DBFloatField()
    ),
    names.CTR_V: ExpressionWrapper(
        F("sum_clicks") * 1.0 / NullIf(F("sum_video_views"), 0),
        output_field=DBFloatField()
    ),
    names.AVERAGE_CPM: ExpressionWrapper(
        F("sum_cost") * 1.0 / NullIf(F("sum_impressions"), 0) * 1000,
        output_field=DBFloatField()
    ),
    names.AVERAGE_CPV: ExpressionWrapper(
        F("sum_cost") * 1.0 / NullIf(F("sum_video_views"), 0),
        output_field=DBFloatField()
    ),
    names.PROFIT: F("revenue") - F("sum_cost"),
    names.MARGIN: (F("revenue") - F("sum_cost")) / NullIf(F("revenue"), 0),
    names.IMPRESSIONS: F("sum_impressions"),
    names.VIDEO_VIEWS: F("sum_video_views"),
    names.CLICKS: F("sum_clicks"),
    names.COST: F("sum_cost"),
}


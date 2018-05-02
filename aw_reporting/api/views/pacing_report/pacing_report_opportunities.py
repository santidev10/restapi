from rest_framework import serializers
from rest_framework.generics import ListAPIView
from rest_framework.serializers import Serializer

from aw_reporting.api.views.pacing_report.pacing_report_helper import \
    PacingReportHelper
from aw_reporting.api.views.pagination import PacingReportOpportunitiesPaginator
from aw_reporting.models.salesforce_constants import DynamicPlacementType, \
    SalesForceGoalType
from aw_reporting.reports.pacing_report import PacingReport, get_chart_data, \
    populate_daily_delivery_data
from utils.datetime import now_in_default_tz
from utils.lang import flatten


class PercentField(serializers.FloatField):
    def to_representation(self, value):
        return super(PercentField, self).to_representation(value) * 100.


class PacingReportOpportunitiesSerializer(Serializer):
    id = serializers.CharField(max_length=20)
    category = serializers.SerializerMethodField()
    region = serializers.SerializerMethodField()
    chart_data = serializers.SerializerMethodField()
    margin = PercentField()
    pacing = PercentField()
    plan_cost = serializers.FloatField()
    cost = serializers.FloatField()
    cpv = serializers.FloatField()
    cpm = serializers.FloatField()
    impressions = serializers.IntegerField()
    video_views = serializers.IntegerField()
    video_view_rate = PercentField()
    has_dynamic_placements = serializers.BooleanField()

    status = serializers.CharField(max_length=200)
    notes = serializers.CharField(max_length=200)
    plan_cpv = serializers.CharField(max_length=200)
    plan_cpm = serializers.CharField(max_length=200)
    goal_type_ids = serializers.CharField(max_length=200)
    goal_type = serializers.CharField(max_length=200)
    start = serializers.CharField(max_length=200)
    cannot_roll_over = serializers.CharField(max_length=200)
    plan_impressions = serializers.CharField(max_length=200)
    ad_ops = serializers.CharField(max_length=200)
    name = serializers.CharField(max_length=200)
    thumbnail = serializers.CharField(max_length=200)
    is_completed = serializers.CharField(max_length=200)
    margin_quality = serializers.CharField(max_length=200)
    sales = serializers.CharField(max_length=200)
    pacing_quality = serializers.CharField(max_length=200)
    plan_video_views = serializers.CharField(max_length=200)
    am = serializers.CharField(max_length=200)
    dynamic_placements_types = serializers.CharField(max_length=200)
    end = serializers.CharField(max_length=200)
    ctr = serializers.CharField(max_length=200)
    is_upcoming = serializers.CharField(max_length=200)
    margin_direction = serializers.CharField(max_length=200)
    video_view_rate_quality = serializers.CharField(max_length=200)
    ctr_quality = serializers.CharField(max_length=200)
    apex_deal = serializers.CharField(max_length=200)
    pacing_direction = serializers.CharField(max_length=200)
    bill_of_third_party_numbers = serializers.CharField(max_length=200)

    def __init__(self, *args, **kwargs):
        super(PacingReportOpportunitiesSerializer, self).__init__(*args,
                                                                  **kwargs)
        self.today = now_in_default_tz().date()
        self._populate_daily_data()

    def _populate_daily_data(self):
        opportunities = self.instance
        all_flights = flatten(obj["flights"] for obj in opportunities)
        populate_daily_delivery_data(all_flights)

    def get_category(self, obj):
        return obj["category"]

    def get_region(self, obj):
        return obj["region"]

    def get_chart_data(self, obj):
        flights = obj["flights"]
        today = self.today
        chart_data = {}
        cpv_flights = [f for f in flights if
                       f["placement__goal_type_id"] == 1]
        if cpv_flights:
            chart_data["cpv"] = get_chart_data(flights=cpv_flights,
                                               today=today)

        cpm_flights = [f for f in flights if
                       f["placement__goal_type_id"] == 0]
        if cpm_flights:
            chart_data["cpm"] = get_chart_data(flights=cpm_flights,
                                               today=today)

        budget_flights = [
            f for f in flights
            if
            f["placement__goal_type_id"] == SalesForceGoalType.HARD_COST and
            f["placement__dynamic_placement"] in (
                DynamicPlacementType.BUDGET,
                DynamicPlacementType.SERVICE_FEE,
                DynamicPlacementType.RATE_AND_TECH_FEE)
        ]
        if budget_flights:
            chart_data["budget"] = get_chart_data(
                flights=budget_flights, today=today)
        return chart_data


class PacingReportOpportunitiesApiView(ListAPIView, PacingReportHelper):
    serializer_class = PacingReportOpportunitiesSerializer
    pagination_class = PacingReportOpportunitiesPaginator

    def get_queryset(self):
        report = PacingReport()
        opportunities = report.get_opportunities(self.request.GET)
        sort_by = self.request.GET.get("sort_by")
        if sort_by is not None:
            reverse = False
            if sort_by.startswith("-"):
                reverse = True
                sort_by = sort_by[1:]

            if sort_by == "account":
                sort_by = "name"

            if sort_by in (
                    "margin", "pacing", "plan_cost", "plan_cpm", "plan_cpv",
                    "plan_impressions", "plan_video_views", "cost", "cpm",
                    "cpv", "impressions", "video_views", "name",
            ):
                if sort_by == "name":
                    def sort_key(i):
                        return i[sort_by].lower()
                else:
                    def sort_key(i):
                        return -1 if i[sort_by] is None else i[sort_by]

                opportunities = list(sorted(
                    opportunities, key=sort_key, reverse=reverse,
                ))
        return opportunities

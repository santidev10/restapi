from rest_framework import serializers
from rest_framework.serializers import Serializer

from aw_reporting.api.serializers.fields.percent_field import PercentField
from aw_reporting.api.serializers.fields.simple_field import SimpleField
from aw_reporting.models import SalesForceGoalType
from aw_reporting.models.salesforce_constants import DynamicPlacementType
from aw_reporting.reports.pacing_report import populate_daily_delivery_data, \
    get_chart_data
from utils.datetime import now_in_default_tz
from utils.lang import flatten


class PacingReportOpportunitiesSerializer(Serializer):
    ad_ops = SimpleField()
    am = SimpleField()
    apex_deal = serializers.BooleanField()
    bill_of_third_party_numbers = serializers.CharField(max_length=200)
    cannot_roll_over = serializers.BooleanField()
    category = serializers.SerializerMethodField()
    chart_data = serializers.SerializerMethodField()
    cost = serializers.FloatField()
    cpm = serializers.FloatField()
    cpv = serializers.FloatField()
    ctr = serializers.FloatField()
    ctr_quality = serializers.IntegerField()
    dynamic_placements_types = SimpleField()
    end = serializers.DateField()
    goal_type = serializers.CharField(max_length=200)
    goal_type_ids = SimpleField()
    has_dynamic_placements = serializers.BooleanField()
    id = serializers.CharField(max_length=20)
    impressions = serializers.IntegerField()
    is_completed = serializers.BooleanField()
    is_upcoming = serializers.BooleanField()
    margin = PercentField()
    margin_direction = serializers.IntegerField()
    margin_quality = serializers.IntegerField()
    name = serializers.CharField(max_length=250)
    notes = SimpleField()
    pacing = PercentField()
    pacing_direction = serializers.IntegerField()
    pacing_quality = serializers.IntegerField()
    plan_cost = serializers.FloatField()
    plan_cpm = serializers.FloatField()
    plan_cpv = serializers.FloatField()
    plan_impressions = serializers.IntegerField()
    plan_video_views = serializers.FloatField()
    region = serializers.SerializerMethodField()
    sales = SimpleField()
    start = serializers.DateField()
    status = serializers.CharField(max_length=10)
    thumbnail = serializers.CharField(max_length=200)
    video_view_rate = PercentField()
    video_view_rate_quality = serializers.IntegerField()
    video_views = serializers.IntegerField()

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
from rest_framework.serializers import IntegerField, BooleanField, CharField, \
    FloatField, DateField, SerializerMethodField
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
    apex_deal = BooleanField()
    bill_of_third_party_numbers = CharField(max_length=200)
    cannot_roll_over = BooleanField()
    category = SerializerMethodField()
    chart_data = SerializerMethodField()
    cost = FloatField()
    cpm = FloatField()
    cpv = FloatField()
    ctr = PercentField()
    ctr_quality = IntegerField()
    dynamic_placements_types = SimpleField()
    end = DateField()
    goal_type = CharField(max_length=200)
    goal_type_ids = SimpleField()
    has_dynamic_placements = BooleanField()
    id = CharField(max_length=20)
    impressions = IntegerField()
    is_completed = BooleanField()
    is_upcoming = BooleanField()
    margin = PercentField()
    margin_direction = IntegerField()
    margin_quality = IntegerField()
    name = CharField(max_length=250)
    notes = SimpleField()
    pacing = PercentField()
    pacing_direction = IntegerField()
    pacing_quality = IntegerField()
    plan_cost = FloatField()
    plan_cpm = FloatField()
    plan_cpv = FloatField()
    plan_impressions = IntegerField()
    plan_video_views = FloatField()
    region = SerializerMethodField()
    sales = SimpleField()
    start = DateField()
    status = CharField(max_length=10)
    thumbnail = CharField(max_length=200)
    video_view_rate = PercentField()
    video_view_rate_quality = IntegerField()
    video_views = IntegerField()

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
                       f["placement__goal_type_id"] == SalesForceGoalType.CPV
                       and f["placement__dynamic_placement"] is None]
        if cpv_flights:
            chart_data["cpv"] = get_chart_data(flights=cpv_flights,
                                               today=today)

        cpm_flights = [f for f in flights if
                       f["placement__goal_type_id"] == SalesForceGoalType.CPM
                       and f["placement__dynamic_placement"] is None]
        if cpm_flights:
            chart_data["cpm"] = get_chart_data(flights=cpm_flights,
                                               today=today)

        budget_flights = [
            f for f in flights
            if f["placement__dynamic_placement"] in (
                DynamicPlacementType.BUDGET,
                DynamicPlacementType.SERVICE_FEE,
                DynamicPlacementType.RATE_AND_TECH_FEE)
        ]
        if budget_flights:
            chart_data["budget"] = get_chart_data(
                flights=budget_flights, today=today)
        return chart_data

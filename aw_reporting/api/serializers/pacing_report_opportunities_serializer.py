from rest_framework.fields import DictField
from rest_framework.serializers import BooleanField
from rest_framework.serializers import CharField
from rest_framework.serializers import DateField
from rest_framework.serializers import DateTimeField
from rest_framework.serializers import FloatField
from rest_framework.serializers import IntegerField
from rest_framework.serializers import Serializer
from rest_framework.serializers import SerializerMethodField

from aw_reporting.models import SalesForceGoalType
from aw_reporting.models.salesforce_constants import DynamicPlacementType
from aw_reporting.reports.pacing_report import get_chart_data
from aw_reporting.reports.pacing_report import populate_daily_delivery_data
from utils.datetime import now_in_default_tz
from utils.lang import flatten
from utils.serializers.fields import PercentField
from utils.serializers.fields import SimpleField


class PacingReportOpportunitiesSerializer(Serializer):
    ad_ops = SimpleField()
    am = SimpleField()
    apex_deal = BooleanField()
    billing_server = CharField(max_length=30)
    cannot_roll_over = BooleanField()
    category = DictField()
    cost = FloatField()
    cpm = FloatField()
    cpv = FloatField()
    cpm_buffer = IntegerField()
    cpv_buffer = IntegerField()
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
    aw_update_time = DateTimeField()
    margin_cap_required = BooleanField()

    def get_region(self, obj):
        territory = obj["territory"]
        return dict(
            id=territory,
            name=territory,
        )

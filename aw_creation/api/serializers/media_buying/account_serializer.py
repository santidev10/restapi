from rest_framework import serializers

from aw_creation.api.serializers.common.stats_aggregator import stats_aggregator
from aw_creation.api.serializers import DashboardAccountCreationListSerializer
from aw_reporting.reports.pacing_report import PacingReport
from aw_reporting.models import Account
from aw_reporting.models import Campaign
from aw_reporting.models.salesforce_constants import SalesForceGoalTypeStr


class AccountMediaBuyingSerializer(DashboardAccountCreationListSerializer):
    stats_aggregations = stats_aggregator(ad_group_stats_prefix="ad_groups__statistics__")

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        pacing_report = PacingReport()
        try:
            account_id = self.instance.account.id
            self.salesforce = pacing_report.get_opportunities({}, user=self.user, aw_cid=[account_id])[0]
        except (Account.DoesNotExist, IndexError):
            self.salesforce = {}

    cid = serializers.SerializerMethodField()
    margin = serializers.SerializerMethodField()
    projected_margin = serializers.SerializerMethodField()
    status = serializers.SerializerMethodField()

    class Meta(DashboardAccountCreationListSerializer.Meta):
        fields = DashboardAccountCreationListSerializer.Meta.fields + (
            "cid",
            "margin",
            "projected_margin",
            "status",
        )

    def get_status(self, obj):
        status = "Not Running"
        try:
            exists = Campaign.objects.filter(account=obj.account, status="running").exists()
            if exists:
                status = "Running"
        except Account.DoesNotExist:
            pass
        return status

    def get_cid(self, obj):
        try:
            cid = obj.account.id
        except Account.DoesNotExist:
            cid = None
        return cid

    def get_margin(self, _):
        margin = self._get_salesforce_value("margin")
        return margin

    def get_projected_margin(self, _):
        projected_margin = None
        try:
            spend = self._get_stats_value("cost")
            plan_units, delivered_units, average_rate = self._get_plan_config()
            units_remaining = plan_units - delivered_units
            client_cost = self._get_salesforce_value("plan_cost")
            projected_margin = (spend + average_rate * units_remaining) / client_cost
        except TypeError:
            pass
        return projected_margin

    def get_clicks(self, _):
        clicks = self._get_stats_value("clicks")
        return clicks

    def _get_salesforce_value(self, key):
        try:
            value = self.salesforce[key]
        except KeyError:
            value = None
        return value

    def _get_stats_value(self, key):
        try:
            value = self.stats[self.instance.id][key]
        except KeyError:
            value = None
        return value

    def _get_plan_config(self):
        plan_units = None
        delivered_units = None
        average_rate = None
        try:
            if self.salesforce["goal_type"] == SalesForceGoalTypeStr.CPM:
                plan_units = self._get_salesforce_value("plan_impressions")
                delivered_units = self._get_stats_value("impressions")
                average_rate = self._get_stats_value("average_cpm")
            else:
                plan_units = self._get_salesforce_value("plan_video_views")
                delivered_units = self._get_stats_value("video_views")
                average_rate = self._get_stats_value("average_cpv")
        except KeyError:
            pass
        return plan_units, delivered_units, average_rate

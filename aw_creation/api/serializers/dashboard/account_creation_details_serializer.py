from datetime import date
from django.db.models import Sum, Q
from rest_framework.fields import SerializerMethodField

from aw_creation.api.serializers import DashboardAccountCreationListSerializer
from aw_creation.models import AccountCreation
from aw_reporting.models import AdGroup, Opportunity, OpPlacement
from utils.serializers.fields import StatField


class DashboardAccountCreationDetailsSerializer(DashboardAccountCreationListSerializer):
    clicks_website = StatField()
    clicks_call_to_action_overlay = StatField()
    clicks_app_store = StatField()
    clicks_cards = StatField()
    clicks_end_cap = StatField()
    hide_click_types = SerializerMethodField()

    show_click_types_after_date = date(year=2018, month=9, day=14)

    class Meta:
        model = AccountCreation
        fields = (
            "clicks_website",
            "clicks_call_to_action_overlay",
            "clicks_app_store",
            "clicks_cards",
            "clicks_end_cap",
            "hide_click_types",
        ) + DashboardAccountCreationListSerializer.Meta.fields

    def _get_stats(self, account_creation_ids):
        stats = super(DashboardAccountCreationDetailsSerializer, self)._get_stats(account_creation_ids)
        for account_creation_id in stats.keys():
            stats_value = stats[account_creation_id]
            clicks_data = AdGroup.objects.filter(
                campaign__account__account_creation__id=account_creation_id).aggregate(
                clicks_website=Sum("clicks_website"),
                clicks_call_to_action_overlay=Sum("clicks_call_to_action_overlay"),
                clicks_app_store=Sum("clicks_app_store"),
                clicks_cards=Sum("clicks_cards"),
                clicks_end_cap=Sum("clicks_end_cap")
            )
            stats_value.update(clicks_data)
        return stats

    def get_hide_click_types(self, obj):
        apex_opportunities_ids = Opportunity.objects.filter(apex_deal=True).values_list("id", flat=True)
        apex_accounts_creations_ids = OpPlacement.objects.filter(
            Q(opportunity_id__in=apex_opportunities_ids) & Q(adwords_campaigns__id__isnull=False)).values_list(
            "adwords_campaigns__account__account_creation__id", flat=True)
        if obj.id in apex_accounts_creations_ids:
            return False
        account_end_date = self.get_end(obj)
        if account_end_date is None:
            return True
        return account_end_date < self.show_click_types_after_date

from datetime import date

from django.db.models import Sum

from aw_creation.api.serializers import AnalyticsAccountCreationListSerializer
from aw_creation.models import AccountCreation
from aw_reporting.models import AdGroup
from utils.serializers.fields import StatField


class AnalyticsCreationDetailsSerializer(AnalyticsAccountCreationListSerializer):
    clicks_website = StatField()
    clicks_call_to_action_overlay = StatField()
    clicks_app_store = StatField()
    clicks_cards = StatField()
    clicks_end_cap = StatField()

    show_click_types_after_date = date(year=2018, month=9, day=14)

    class Meta:
        model = AccountCreation
        fields = (
            "clicks_website",
            "clicks_call_to_action_overlay",
            "clicks_app_store",
            "clicks_cards",
            "clicks_end_cap",
        ) + AnalyticsAccountCreationListSerializer.Meta.fields

    def _get_stats(self, account_creation_ids):
        stats = super(AnalyticsCreationDetailsSerializer, self)._get_stats(account_creation_ids)
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
            for key, value in clicks_data.items():
                if value is None:
                    clicks_data[key] = 0
            stats_value.update(clicks_data)
        return stats

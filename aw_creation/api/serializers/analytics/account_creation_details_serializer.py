from aw_creation.api.serializers import AnalyticsAccountCreationListSerializer
from aw_creation.api.serializers.analytics.base_account_creation_serializer import BaseAccountCreationSerializer
from aw_reporting.models import base_stats_aggregator
from aw_reporting.models.ad_words.calculations import click_stats_aggregations
from utils.serializers.fields import StatField


class AnalyticsAccountCreationDetailsSerializer(BaseAccountCreationSerializer):
    clicks_website = StatField()
    clicks_call_to_action_overlay = StatField()
    clicks_app_store = StatField()
    clicks_cards = StatField()
    clicks_end_cap = StatField()

    stats_aggregations = {
        **base_stats_aggregator(),
        **click_stats_aggregations(),
    }

    class Meta(BaseAccountCreationSerializer.Meta):
        fields = (
                     "clicks_website",
                     "clicks_call_to_action_overlay",
                     "clicks_app_store",
                     "clicks_cards",
                     "clicks_end_cap",
                 ) + AnalyticsAccountCreationListSerializer.Meta.fields

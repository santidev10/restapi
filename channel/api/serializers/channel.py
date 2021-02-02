from django.contrib.auth import get_user_model
from rest_framework.fields import SerializerMethodField

from audit_tool.models import BlacklistItem
from es_components.constants import Sections
from userprofile.constants import StaticPermissions
from utils.brand_safety import get_brand_safety_data
from utils.datetime import date_to_chart_data_str
from utils.es_components_api_utils import BlackListSerializerMixin
from utils.es_components_api_utils import ESDictSerializer
from utils.es_components_api_utils import VettedStatusSerializerMixin
from utils.serializers.fields import ParentDictValueField


class ChannelSerializer(VettedStatusSerializerMixin, BlackListSerializerMixin, ESDictSerializer):
    chart_data = SerializerMethodField()
    brand_safety_data = SerializerMethodField()

    # Controlled by permissions
    blacklist_data = ParentDictValueField("blacklist_data", source="main.id")
    vetted_status = SerializerMethodField()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        user = self.context.get("user")

        # Dynamically remove fields not allowed by user permissions
        if self.fields and isinstance(user, get_user_model()):
            if not user.has_permission(StaticPermissions.RESEARCH__VETTING_DATA):
                self.fields.pop("vetted_status", None)
            if not user.has_permission(StaticPermissions.RESEARCH__BRAND_SUITABILITY_HIGH_RISK):
                self.fields.pop("blacklist_data", None)

    def to_representation(self, instance):
        data = super().to_representation(instance)
        data.pop(Sections.TASK_US_DATA, None)
        return data

    def get_chart_data(self, channel):
        return get_chart_data(channel)

    def get_brand_safety_data(self, channel):
        return get_brand_safety_data(channel.brand_safety.overall_score)


def get_chart_data(channel):
    if not hasattr(channel, "stats"):
        return None

    items = []
    subscribers_raw_history = channel.stats.subscribers_raw_history.to_dict()
    views_raw_history = channel.stats.views_raw_history.to_dict()
    history_dates = set(list(subscribers_raw_history.keys()) + list(views_raw_history.keys()))

    for history_date in sorted(list(history_dates)):
        items.append({
            "created_at": date_to_chart_data_str(history_date),
            "subscribers": subscribers_raw_history.get(history_date),
            "views": views_raw_history.get(history_date)
        })
    return items

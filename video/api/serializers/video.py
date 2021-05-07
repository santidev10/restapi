import re

from django.contrib.auth import get_user_model
from rest_framework.fields import SerializerMethodField

from es_components.constants import Sections
from userprofile.constants import StaticPermissions
from utils.brand_safety import get_brand_safety_data
from utils.datetime import date_to_chart_data_str
from utils.es_components_api_utils import BlackListSerializerMixin
from utils.es_components_api_utils import ESDictSerializer
from utils.es_components_api_utils import VettedStatusSerializerMixin
from utils.serializers.fields import ParentDictValueField
from video.api.serializers.video_transcript_serializer_mixin import VideoTranscriptSerializerMixin


class VideoSerializer(VideoTranscriptSerializerMixin, VettedStatusSerializerMixin, BlackListSerializerMixin,
                      ESDictSerializer):

    chart_data = SerializerMethodField()
    transcript = SerializerMethodField()
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
        try:
            data["channel"]["thumbnail_image_url"] = self.context["thumbnail_image_url"].get(instance.channel.id)
        except KeyError:
            pass
        data.pop(Sections.TASK_US_DATA, None)
        return data

    def get_chart_data(self, video):
        if not video.stats:
            return []

        chart_data = []
        views_raw_history = video.stats.views_raw_history.to_dict()
        likes_raw_history = video.stats.likes_raw_history.to_dict()
        dislikes_raw_history = video.stats.dislikes_raw_history.to_dict()
        comments_raw_history = video.stats.comments_raw_history.to_dict()

        history_dates = set(list(views_raw_history.keys()) + list(likes_raw_history.keys()) +
                            list(dislikes_raw_history.keys()) + list(comments_raw_history.keys()))

        for history_date in sorted(list(history_dates)):
            chart_data.append({
                "created_at": date_to_chart_data_str(history_date),
                "views": views_raw_history.get(history_date),
                "likes": likes_raw_history.get(history_date),
                "dislikes": dislikes_raw_history.get(history_date),
                "comments": comments_raw_history.get(history_date)
            })

        return chart_data

    @staticmethod
    def get_brand_safety_data(channel) -> dict:
        return get_brand_safety_data(channel.brand_safety.overall_score)

    def update(self, instance, validated_data):
        raise NotImplementedError

    def create(self, validated_data):
        raise NotImplementedError

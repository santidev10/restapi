import re

from django.contrib.auth import get_user_model
from elasticsearch_dsl.utils import AttrList
from rest_framework.fields import SerializerMethodField

from brand_safety.languages import TRANSCRIPTS_LANGUAGE_PRIORITY
from es_components.constants import Sections
from es_components.models.video import Video
from userprofile.constants import StaticPermissions
from utils.brand_safety import get_brand_safety_data
from utils.datetime import date_to_chart_data_str
from utils.es_components_api_utils import BlackListSerializerMixin
from utils.es_components_api_utils import ESDictSerializer
from utils.es_components_api_utils import VettedStatusSerializerMixin
from utils.serializers.fields import ParentDictValueField


REGEX_TO_REMOVE_TIMEMARKS = r"^\s*$|((\r\n|\n|\r|\,|)(\d+(\:\d+\:\d+[.,]\d+|))(\s+-->\s+\d+\:\d+\:\d+[.,]\d+|))"


class VideoSerializer(ESDictSerializer, VettedStatusSerializerMixin, BlackListSerializerMixin):
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

    def get_transcript(self, video: Video) -> str:
        """
        given a Video instance, get the transcript from the video
        :param video:
        :return: str
        """
        text = ""
        try:
            vid_lang_code = video.general_data.lang_code
        # pylint: disable=broad-except
        except Exception:
            vid_lang_code = "en"
        # pylint: enable=broad-except
        # copy by value (instead of ref), to prevent priorities list morphing
        lang_code_priorities = TRANSCRIPTS_LANGUAGE_PRIORITY.copy()
        if vid_lang_code and isinstance(vid_lang_code, str):
            lang_code_priorities.insert(0, vid_lang_code.lower())
        if "captions" in video and "items" in video.captions:
            validated_caption_items = self._validate_caption_items(video.captions.items)
            text = self._get_best_available_transcript(lang_code_priorities=lang_code_priorities,
                                                       captions_items=validated_caption_items)
        if not text and "custom_captions" in video and "items" in video.custom_captions:
            validated_caption_items = self._validate_caption_items(video.custom_captions.items)
            text = self._get_best_available_transcript(lang_code_priorities=lang_code_priorities,
                                                       captions_items=validated_caption_items)
        transcript = re.sub(REGEX_TO_REMOVE_TIMEMARKS, "", text or "")
        return transcript

    @staticmethod
    def _validate_caption_items(caption_items: AttrList) -> list:
        """
        get a list of caption items and validate each item, returns only valid items
        :param caption_items:
        :return:
        """
        validated = [item for item in caption_items
                     if hasattr(item, "language_code")
                     and isinstance(item.language_code, str)]
        return validated

    def _get_best_available_transcript(self, lang_code_priorities: list, captions_items: list) -> str:
        """
        given a list of language codes ordered by priority, and a list of caption items, get the highest ranked caption
        text by language priority
        :param lang_code_priorities:
        :param captions_items:
        :return: str
        """
        text = ""
        if not len(captions_items):
            return text

        # Trim lang_codes to first 2 characters because custom_captions often have lang_codes like "en-US" or "en-UK"
        best_lang_code = self._get_best_available_language(lang_code_priorities, captions_items)
        for item in captions_items:
            if item.language_code.split("-")[0].lower() == best_lang_code:
                text = item.text
                break
        return text

    @staticmethod
    def _get_best_available_language(lang_code_priorities: list, captions_items: list) -> str:
        """
        given a list of language codes ordered by priority, get the first available language code
        :param lang_code_priorities:
        :param captions_items:
        :return:
        """
        available_lang_codes = [item.language_code.split("-")[0].lower() for item in captions_items]
        for lang_code in lang_code_priorities:
            if lang_code in available_lang_codes:
                return lang_code
        return captions_items[0].language_code

    @staticmethod
    def get_brand_safety_data(channel) -> dict:
        return get_brand_safety_data(channel.brand_safety.overall_score)

    def update(self, instance, validated_data):
        raise NotImplementedError

    def create(self, validated_data):
        raise NotImplementedError

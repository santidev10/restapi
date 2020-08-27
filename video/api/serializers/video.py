import re

from rest_framework.fields import SerializerMethodField

from audit_tool.models import BlacklistItem
from brand_safety.languages import TRANSCRIPTS_LANGUAGE_PRIORITY
from utils.brand_safety import get_brand_safety_data
from utils.datetime import date_to_chart_data_str
from utils.es_components_api_utils import ESDictSerializer
from utils.es_components_api_utils import VettedStatusSerializerMixin
from utils.serializers.fields import ParentDictValueField


REGEX_TO_REMOVE_TIMEMARKS = r"^\s*$|((\r\n|\n|\r|\,|)(\d+(\:\d+\:\d+[.,]\d+|))(\s+-->\s+\d+\:\d+\:\d+[.,]\d+|))"


class VideoSerializer(ESDictSerializer):
    chart_data = SerializerMethodField()
    transcript = SerializerMethodField()
    brand_safety_data = SerializerMethodField()

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

    def get_transcript(self, video):
        text = ""
        try:
            vid_lang_code = video.general_data.lang_code
        # pylint: disable=broad-except
        except Exception:
            vid_lang_code = "en"
        # pylint: enable=broad-except
        lang_code_priorities = TRANSCRIPTS_LANGUAGE_PRIORITY
        if vid_lang_code:
            lang_code_priorities.insert(0, vid_lang_code.lower())
        if "captions" in video and "items" in video.captions:
            text = self.get_best_available_transcript(lang_code_priorities=lang_code_priorities,
                                                      captions_items=video.captions.items)
        if not text and "custom_captions" in video and "items" in video.custom_captions:
            text = self.get_best_available_transcript(lang_code_priorities=lang_code_priorities,
                                                      captions_items=video.custom_captions.items)
        transcript = re.sub(REGEX_TO_REMOVE_TIMEMARKS, "", text or "")
        return transcript

    def get_best_available_transcript(self, lang_code_priorities, captions_items):
        text = ""
        # Trim lang_codes to first 2 characters because custom_captions often have lang_codes like "en-US" or "en-UK"
        best_lang_code = self.get_best_available_language(lang_code_priorities, captions_items)
        for item in captions_items:
            if item.language_code.split("-")[0].lower() == best_lang_code:
                text = item.text
                break
        return text

    @staticmethod
    def get_best_available_language(lang_code_priorities, captions_items):
        available_lang_codes = [item.language_code.split("-")[0].lower() for item in captions_items]
        for lang_code in lang_code_priorities:
            if lang_code in available_lang_codes:
                return lang_code
        return captions_items[0].language_code

    def get_brand_safety_data(self, channel):
        return get_brand_safety_data(channel.brand_safety.overall_score)

    def update(self, instance, validated_data):
        raise NotImplementedError

    def create(self, validated_data):
        raise NotImplementedError


# todo: duplicates ChannelWithBlackListSerializer
class VideoWithBlackListSerializer(VideoSerializer):
    blacklist_data = ParentDictValueField("blacklist_data", source="main.id")

    def __init__(self, instance, *args, **kwargs):
        super(VideoWithBlackListSerializer, self).__init__(instance, *args, **kwargs)
        self.blacklist_data = {}
        if instance:
            videos = instance if isinstance(instance, list) else [instance]
            self.blacklist_data = self.fetch_blacklist_items(videos)

    def update(self, instance, validated_data):
        raise NotImplementedError

    def create(self, validated_data):
        raise NotImplementedError

    def fetch_blacklist_items(self, videos):
        doc_ids = [doc.meta.id for doc in videos]
        blacklist_items = BlacklistItem.get(doc_ids, BlacklistItem.VIDEO_ITEM)
        blacklist_items_by_id = {
            item.item_id: {
                "blacklist_data": item.to_dict()
            } for item in blacklist_items
        }
        return blacklist_items_by_id


class VideoWithVettedStatusSerializer(VettedStatusSerializerMixin, VideoSerializer):
    vetted_status = SerializerMethodField()


class VideoAdminSerializer(VettedStatusSerializerMixin, VideoWithBlackListSerializer):
    vetted_status = SerializerMethodField()

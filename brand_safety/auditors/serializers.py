import re

from rest_framework.serializers import CharField
from rest_framework.serializers import Serializer
from rest_framework.serializers import SerializerMethodField

from video.api.serializers.video import REGEX_TO_REMOVE_TIMEMARKS
from brand_safety.languages import TRANSCRIPTS_LANGUAGE_PRIORITY


class BrandSafetyChannelSerializer(Serializer):
    id = CharField(source="main.id")
    title = CharField(source="general_data.title", default="")
    description = CharField(source="general_data.description", default="")
    video_tags = SerializerMethodField()
    updated_at = SerializerMethodField()

    def get_video_tags(self, obj):
        tags = " ".join(getattr(obj.general_data, "video_tags", []))
        return tags

    def get_updated_at(self, obj):
        """
        "Preserve updated_at datetime object to calculate time differences
        :param obj:
        :return:
        """
        return obj.brand_safety.updated_at


class BrandSafetyVideoSerializer(Serializer):
    id = CharField(source="main.id")
    channel_id = CharField(source="channel.id", default="")
    channel_title = CharField(source="channel.title", default="")
    title = CharField(source="general_data.title", default="")
    description = CharField(source="general_data.description", default="")
    tags = SerializerMethodField()
    transcript = SerializerMethodField()

    def get_tags(self, obj):
        tags = " ".join(getattr(obj.general_data, "tags", []))
        return tags

    def get_transcript(self, video):
        text = ""
        try:
            vid_lang_code = video.general_data.lang_code
        except Exception as e:
            vid_lang_code = 'en'

        if 'captions' in video and 'items' in video.captions:
            for item in video.captions.items:
                if item.language_code.lower() == vid_lang_code or item.language_code[:2].lower() == vid_lang_code:
                    text = item.text
                    break
            if not text:
                text = video.captions.items[0].text
        if not text and 'custom_captions' in video and 'items' in video.custom_captions:
            lang_code_priorities = TRANSCRIPTS_LANGUAGE_PRIORITY.insert(0, vid_lang_code)
            available_lang_codes = [item.language_code[:2].lower() for item in video.custom_captions.items]
            best_lang_code = None
            for lang_code in lang_code_priorities:
                if lang_code in available_lang_codes:
                    best_lang_code = lang_code
                    break
            if best_lang_code:
                for item in video.custom_captions.items:
                    if item.language_code[:2].lower() == vid_lang_code:
                        text = item.text
                        break
            else:
                text = video.custom_captions.items[0].text
        transcript = re.sub(REGEX_TO_REMOVE_TIMEMARKS, "", text or "")
        return transcript

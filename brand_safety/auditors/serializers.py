import re

from rest_framework.serializers import CharField
from rest_framework.serializers import Serializer
from rest_framework.serializers import SerializerMethodField

from brand_safety.languages import TRANSCRIPTS_LANGUAGE_PRIORITY
from video.api.serializers.video import REGEX_TO_REMOVE_TIMEMARKS


class BrandSafetyChannelSerializer(Serializer):
    id = CharField(source="main.id")
    title = CharField(source="general_data.title", default="")
    description = CharField(source="general_data.description", default="")
    is_vetted = SerializerMethodField()
    video_tags = SerializerMethodField()
    updated_at = SerializerMethodField()

    def get_is_vetted(self, obj):
        is_vetted = False
        if obj.task_us_data:
            is_vetted = True
        return is_vetted

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
    language = CharField(source="general_data.lang_code", default="")
    is_vetted = SerializerMethodField()
    tags = SerializerMethodField()
    transcript = SerializerMethodField()
    transcript_language = SerializerMethodField()

    def get_is_vetted(self, obj):
        is_vetted = False
        if obj.task_us_data:
            is_vetted = True
        return is_vetted

    def get_tags(self, obj):
        tags = " ".join(getattr(obj.general_data, "tags", []))
        return tags

    def get_transcript(self, video):
        text = ""
        try:
            vid_lang_code = video.general_data.lang_code
        # pylint: disable=broad-except
        except Exception as e:
        # pylint: enable=broad-except
            vid_lang_code = 'en'
        lang_code_priorities = TRANSCRIPTS_LANGUAGE_PRIORITY
        if vid_lang_code:
            lang_code_priorities.insert(0, vid_lang_code.lower())
        if 'captions' in video and 'items' in video.captions:
            text = self.get_best_available_transcript(lang_code_priorities=lang_code_priorities,
                                                      captions_items=video.captions.items)
        if not text and 'custom_captions' in video and 'items' in video.custom_captions:
            text = self.get_best_available_transcript(lang_code_priorities=lang_code_priorities,
                                                      captions_items=video.custom_captions.items)
        transcript = re.sub(REGEX_TO_REMOVE_TIMEMARKS, "", text or "")
        return transcript

    def get_transcript_language(self, video):
        try:
            vid_lang_code = video.general_data.lang_code
        # pylint: disable=broad-except
        except Exception as e:
        # pylint: enable=broad-except
            vid_lang_code = 'en'
        lang_code_priorities = TRANSCRIPTS_LANGUAGE_PRIORITY
        if vid_lang_code:
            lang_code_priorities.insert(0, vid_lang_code.lower())
        transcript_language = None
        if 'captions' in video and 'items' in video.captions:
            transcript_language = self.get_best_available_language(lang_code_priorities=lang_code_priorities,
                                                                   captions_items=video.captions.items)
        if not transcript_language and 'custom_captions' in video and 'items' in video.custom_captions:
            transcript_language = self.get_best_available_language(lang_code_priorities=lang_code_priorities,
                                                                   captions_items=video.custom_captions.items)
        return transcript_language

    @staticmethod
    def get_best_available_language(lang_code_priorities, captions_items):
        available_lang_codes = [item.language_code.split('-')[0].lower() for item in captions_items]
        for lang_code in lang_code_priorities:
            if lang_code in available_lang_codes:
                return lang_code
        return captions_items[0].language_code

    def get_best_available_transcript(self, lang_code_priorities, captions_items):
        text = ""
        # Trim lang_codes to first 2 characters because custom_captions often have lang_codes like "en-US" or "en-UK"
        best_lang_code = self.get_best_available_language(lang_code_priorities, captions_items)
        for item in captions_items:
            if item.language_code.split('-')[0].lower() == best_lang_code:
                text = item.text
                break
        return text

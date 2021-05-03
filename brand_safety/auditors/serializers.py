import re
from typing import Tuple
from typing import Union

from rest_framework.serializers import ListField
from rest_framework.serializers import Serializer
from rest_framework.serializers import SerializerMethodField

from brand_safety.languages import TRANSCRIPTS_LANGUAGE_PRIORITY
from video.api.serializers.video import REGEX_TO_REMOVE_TIMEMARKS


class BrandSafetyChannel(Serializer):
    """ Adds attributes to Channel instances """
    video_tags = SerializerMethodField()
    updated_at = SerializerMethodField()
    videos = ListField(default=[])

    def to_representation(self, *_, **__):
        instance = self.instance
        extra_data = super().to_representation(instance)
        for key, val in extra_data.items():
            setattr(instance, key, val)
        return instance

    def get_is_vetted(self, obj):
        is_vetted = False
        if obj.task_us_data.last_vetted_at:
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


class BrandSafetyVideo(Serializer):
    """ Adds attributes to Video instances """
    tags = SerializerMethodField()
    transcript = SerializerMethodField()
    transcript_language = SerializerMethodField()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.lang_code_priorities = TRANSCRIPTS_LANGUAGE_PRIORITY.copy()

    def to_representation(self, instance):
        extra_data = super().to_representation(instance)
        for key, val in extra_data.items():
            setattr(instance, key, val)

        # Set blocklist value if channel is blocklisted
        instance.custom_properties.blocklist = self._is_blocklist(instance)
        return instance

    def _is_blocklist(self, instance):
        is_blocklist = False
        try:
            channel_blocklist = self.context["channels"][instance.channel.id].custom_properties.blocklist
        except KeyError:
            channel_blocklist = False
        if instance.custom_properties.blocklist is True or channel_blocklist is True:
            is_blocklist = True
        return is_blocklist

    def get_tags(self, obj):
        tags = getattr(obj.general_data, "tags", "") or ""
        if not isinstance(tags, str):
            tags = " ".join(tags)
        return tags

    def get_transcript(self, video):
        text = ""
        try:
            vid_lang_code = video.general_data.lang_code
        # pylint: disable=broad-except
        except Exception as e:
        # pylint: enable=broad-except
            vid_lang_code = 'en'
        lang_code_priorities = TRANSCRIPTS_LANGUAGE_PRIORITY.copy()
        if vid_lang_code:
            self.lang_code_priorities.insert(0, vid_lang_code.lower())
            lang_code_priorities.insert(0, vid_lang_code.lower())
        transcripts = self.context.get("transcripts", {}).get(video.main.id)
        if transcripts:
            text = self.get_highest_priority_transcript(transcripts=transcripts)
        else:
            if "captions" in video and "items" in video.captions:
                text = self.get_best_available_transcript(lang_code_priorities=lang_code_priorities,
                                                          captions_items=video.captions.items)
            if not text and "custom_captions" in video and "items" in video.custom_captions:
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
        # copy by value (instead of ref), to prevent priorities list morphing
        lang_code_priorities = TRANSCRIPTS_LANGUAGE_PRIORITY.copy()
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
        try:
            available_lang_codes = [item.language_code.split('-')[0].lower() for item in captions_items]
        except AttributeError:
            return
        for lang_code in lang_code_priorities:
            if lang_code in available_lang_codes:
                return lang_code
        return captions_items[0].language_code

    def get_best_available_transcript(self, lang_code_priorities, captions_items):
        text = ""
        # Trim lang_codes to first 2 characters because custom_captions often have lang_codes like "en-US" or "en-UK"
        best_lang_code = self.get_best_available_language(lang_code_priorities, captions_items)
        for item in captions_items:
            try:
                item_lang = item.language_code.split('-')[0].lower()
            except AttributeError:
                continue
            if item_lang == best_lang_code:
                text = item.text
                break
        return text

    def get_highest_priority_lang_code(self, transcripts: list) -> Union[str, None]:
        """
        equivalent method to get_best_available_language for transcripts from the Transcripts index
        video.custom_captions.transcripts.items is deprecated
        :param transcripts:
        :return: str, None
        """
        available_lang_codes = []
        generator = self._get_transcript_lang_code_generator(transcripts=transcripts)
        for lang_code, transcript in generator:
            available_lang_codes.append(lang_code)
        if not available_lang_codes:
            return

        for lang_code in self.lang_code_priorities:
            if lang_code in available_lang_codes:
                return lang_code
        return transcripts[0].general_data.language_code.split("-")[0].lower()

    def get_highest_priority_transcript(self, transcripts: list) -> str:
        """
        equivalent method to get_best_available_transcript for transcripts from the Transcripts index
        video.custom_captions.transcripts.items is deprecated
        :param transcripts:
        :return:
        """
        # Trim lang_codes to first 2 characters because custom_captions often have lang_codes like "en-US" or "en-UK"
        priority_lang_code = self.get_highest_priority_lang_code(transcripts=transcripts)
        if priority_lang_code is None:
            return ""
        generator = self._get_transcript_lang_code_generator(transcripts=transcripts)
        for transcript_lang_code, transcript in generator:
            if transcript_lang_code == priority_lang_code:
                return transcript.text.value
        return ""

    @staticmethod
    def _get_transcript_lang_code_generator(transcripts: list) -> Tuple[str, str]:
        """
        safe generator to get lang codes and catch invalid transcripts
        :param transcripts: list
        :return: (str, str)
        """
        for transcript in transcripts:
            try:
                raw_lang_code = transcript.general_data.language_code
            except AttributeError:
                continue
            if not raw_lang_code or not isinstance(raw_lang_code, str):
                continue
            transcript_lang_code = raw_lang_code.split("-")[0].lower()
            yield transcript_lang_code, transcript

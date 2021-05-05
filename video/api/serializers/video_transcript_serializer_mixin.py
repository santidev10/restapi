import re
from typing import Tuple
from typing import Union

from elasticsearch_dsl.utils import AttrList

from brand_safety.languages import TRANSCRIPTS_LANGUAGE_PRIORITY


REGEX_TO_REMOVE_TIMEMARKS = r"^\s*$|((\r\n|\n|\r|\,|)(\d+(\:\d+\:\d+[.,]\d+|))(\s+-->\s+\d+\:\d+\:\d+[.,]\d+|))"


class VideoTranscriptSerializerMixin:

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # copy by value (instead of ref), to prevent priorities list morphing
        self.lang_code_priorities = TRANSCRIPTS_LANGUAGE_PRIORITY.copy()

    def get_transcript(self, video):
        """
        serializer method for getting video transcript
        :param video:
        :return:
        """
        text = ""
        try:
            vid_lang_code = video.general_data.lang_code
        except AttributeError:
            vid_lang_code = "en"
        if vid_lang_code:
            self.lang_code_priorities.insert(0, vid_lang_code.lower())
        transcripts = self.context.get("transcripts", {}).get(str(video.main.id))
        if transcripts:
            text = self._get_highest_priority_transcript(transcripts=transcripts)
        else:
            if "captions" in video and "items" in video.captions:
                captions_items = self._validate_caption_items(caption_items=video.captions.items)
                text = self._get_best_available_transcript(captions_items=captions_items)
            if not text and "custom_captions" in video and "items" in video.custom_captions:
                captions_items = self._validate_caption_items(caption_items=video.custom_captions.items)
                text = self._get_best_available_transcript(captions_items=captions_items)
        transcript = re.sub(REGEX_TO_REMOVE_TIMEMARKS, "", text or "")
        return transcript

    def get_transcript_language(self, video):
        """
        serializer method for getting video's transcript language
        :param video:
        :return:
        """
        try:
            vid_lang_code = video.general_data.lang_code
        except AttributeError:
            vid_lang_code = "en"
        if vid_lang_code:
            self.lang_code_priorities.insert(0, vid_lang_code.lower())
        transcripts = self.context.get("transcripts", {}).get(video.main.id)
        if transcripts:
            language = self._get_highest_priority_lang_code(transcripts=transcripts)
        else:
            language = None
            if "captions" in video and "items" in video.captions:
                captions_items = self._validate_caption_items(caption_items=video.captions.items)
                language = self._get_best_available_language(captions_items=captions_items)
            if not language and "custom_captions" in video and "items" in video.custom_captions:
                captions_items = self._validate_caption_items(caption_items=video.custom_captions.items)
                language = self._get_best_available_language(captions_items=captions_items)
        return language

    def _get_best_available_language(self, captions_items) -> Union[str, None]:
        """
        NOTE: Deprecated
        helper function for getting old-style transcript language (attached to video object)
        :param captions_items:
        :return:
        """
        if not captions_items:
            return
        try:
            available_lang_codes = [item.language_code.split('-')[0].lower() for item in captions_items]
        except AttributeError:
            return
        for lang_code in self.lang_code_priorities:
            if lang_code in available_lang_codes:
                return lang_code
        return captions_items[0].language_code

    def _get_best_available_transcript(self, captions_items):
        """
        NOTE: Deprecated
        helper function for getting old-style transcript (attached to video object)
        :param captions_items:
        :return:
        """
        text = ""
        # Trim lang_codes to first 2 characters because custom_captions often have lang_codes like "en-US" or "en-UK"
        best_lang_code = self._get_best_available_language(captions_items)
        for item in captions_items:
            try:
                item_lang = item.language_code.split('-')[0].lower()
            except AttributeError:
                continue
            if item_lang == best_lang_code:
                text = item.text
                break
        return text

    def _get_highest_priority_lang_code(self, transcripts: list) -> Union[str, None]:
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

    def _get_highest_priority_transcript(self, transcripts: list) -> str:
        """
        equivalent method to get_best_available_transcript for transcripts from the Transcripts index
        video.custom_captions.transcripts.items is deprecated
        :param transcripts:
        :return:
        """
        # Trim lang_codes to first 2 characters because custom_captions often have lang_codes like "en-US" or "en-UK"
        priority_lang_code = self._get_highest_priority_lang_code(transcripts=transcripts)
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

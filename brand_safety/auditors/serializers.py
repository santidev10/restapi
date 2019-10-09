import re

from rest_framework.serializers import CharField
from rest_framework.serializers import Serializer
from rest_framework.serializers import SerializerMethodField

from video.api.serializers.video import REGEX_TO_REMOVE_TIMEMARKS
from django.conf.global_settings import LANGUAGES


class BrandSafetyChannelSerializer(Serializer):
    id = CharField(source="main.id")
    title = CharField(source="general_data.title", default="")
    description = CharField(source="general_data.description", default="")
    video_tags = SerializerMethodField()
    updated_at = SerializerMethodField()

    def get_video_tags(self, obj):
        tags = ",".join(getattr(obj.general_data, "video_tags", []))
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
    title = CharField(source="general_data.title", default="")
    description = CharField(source="general_data.description", default="")
    tags = SerializerMethodField()
    transcript = SerializerMethodField()

    def get_tags(self, obj):
        tags = ",".join(getattr(obj.general_data, "tags", []))
        return tags

    def get_transcript(self, video):
        text = ""
        lang_dict = {item[1]: item[0] for item in LANGUAGES}
        try:
            vid_language = video.general_data.language
            vid_lang_code = lang_dict[vid_language.capitalize()]
        except Exception as e:
            vid_lang_code = 'en'

        if 'captions' in video and 'items' in video.captions:
            if len(video.captions.items) == 1:
                text = video.captions.items[0].text
            else:
                for item in video.captions.items:
                    if item.language_code == vid_lang_code:
                        text = item.text
                        break
        if not text and 'custom_transcripts' in video and 'transcripts' in video.custom_transcripts:
            for item in video.custom_transcripts.transcripts:
                if item.language_code == vid_lang_code:
                    text = item.text
                    break
        transcript = re.sub(REGEX_TO_REMOVE_TIMEMARKS, "", text)
        return transcript

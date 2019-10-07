import re

from rest_framework.serializers import CharField
from rest_framework.serializers import Serializer
from rest_framework.serializers import SerializerMethodField

from video.api.serializers.video import REGEX_TO_REMOVE_TIMEMARKS


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
    custom_transcript = SerializerMethodField()

    def get_tags(self, obj):
        tags = ",".join(getattr(obj.general_data, "tags", []))
        return tags

    def get_transcript(self, video):
        en_captions = None
        if 'captions' in video and 'items' in video.captions:
            en_captions = [caption.text for caption in video.captions.items if caption.language_code == "en"]
        elif 'custom_transcripts' in video and 'transcripts' in video.custom_transcripts:
            en_captions = [caption.text for caption in video.custom_transcripts.transcripts if caption.language_code == "en"]
        text = en_captions[0] if en_captions else ""
        print('text: {}'.format(text))
        transcript = re.sub(REGEX_TO_REMOVE_TIMEMARKS, "", text)
        return transcript

    def get_custom_transcript(self, video):
        custom_transcript = None
        if 'custom_transcripts' in video and 'transcripts' in video.custom_transcripts:
            for custom_transcript in video.custom_transcripts.transcripts:
                if custom_transcript.language_code == "en":
                    text = custom_transcript.text
                    custom_transcript = re.sub(REGEX_TO_REMOVE_TIMEMARKS, "", text)
        return custom_transcript

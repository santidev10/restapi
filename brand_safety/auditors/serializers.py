from rest_framework.serializers import ListField
from rest_framework.serializers import Serializer
from rest_framework.serializers import SerializerMethodField

from video.api.serializers.video_transcript_serializer_mixin import VideoTranscriptSerializerMixin


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


class BrandSafetyVideo(VideoTranscriptSerializerMixin, Serializer):
    """ Adds attributes to Video instances """
    tags = SerializerMethodField()
    transcript = SerializerMethodField()
    transcript_language = SerializerMethodField()

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

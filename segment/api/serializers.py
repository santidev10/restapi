"""
Segment api serializers module
"""
from rest_framework.serializers import ModelSerializer, CharField, \
    ValidationError, SerializerMethodField

from segment.models import Segment, AVAILABLE_SEGMENT_TYPES, \
    AVAILABLE_SEGMENT_CATEGORIES, ChannelRelation, VideoRelation


class SegmentCreateSerializer(ModelSerializer):
    """
    Serializer for create segment
    """
    title = CharField(max_length=255, required=True)
    channels_ids = CharField(max_length=255, required=False)
    videos_ids = CharField(max_length=255, required=False)

    class Meta:
        """
        Meta params
        """
        model = Segment
        fields = (
            "title",
            "segment_type",
            "category",
            "channels_ids",
            "videos_ids"
        )

    def validate(self, data):
        """
        Check segment type and category
        """
        # set up channels_ids and videos_ids
        self.channels_ids = data.pop("channels_ids", None)
        self.videos_ids = data.pop("videos_ids", None)
        # segment type
        segment_type = data.get("segment_type")
        if segment_type not in AVAILABLE_SEGMENT_TYPES:
            raise ValidationError(
                "Not valid segment type. Options are: {}".format(
                    ", ".join(AVAILABLE_SEGMENT_TYPES)))
        # segment category
        segment_category = data.get("category")
        user = self.context.get("request").user
        if not user.is_staff and segment_category != "private":
            raise ValidationError(
                "Not valid category. Options are: private")
        elif segment_category not in AVAILABLE_SEGMENT_CATEGORIES:
            raise ValidationError(
                "Not valid category. Options are: {}".format(
                    ", ".join(AVAILABLE_SEGMENT_CATEGORIES)))
        return data

    def save(self, **kwargs):
        """
        Set owner
        """
        segment = super(SegmentCreateSerializer, self).save(**kwargs)
        # set owner
        user = self.context.get("request").user
        segment.owner = user
        segment.save()
        # set channels
        if self.channels_ids is not None:
            channels_ids = self.channels_ids.split(",")
            instances = []
            for channel_id in channels_ids:
                instance, is_created = ChannelRelation.objects.get_or_create(
                    channel_id=channel_id)
                instances.append(instance)
            segment.channels.add(*instances)
        # set videos
        if self.videos_ids is not None:
            videos_ids = self.videos_ids.split(",")
            instances = []
            for video_id in videos_ids:
                instance, is_created = VideoRelation.objects.get_or_create(
                    video_id=video_id)
                instances.append(instance)
            segment.videos.add(*instances)
        return segment


class SegmentSerializer(ModelSerializer):
    """
    Segment retrieve serializer
    """
    is_editable = SerializerMethodField()

    class Meta:
        """
        Meta params
        """
        model = Segment
        fields = (
            "id",
            "title",
            "segment_type",
            "category",
            "statistics",
            "mini_dash_data",
            "is_editable"
        )

    def get_is_editable(self, obj):
        """
        Check segment can be edited
        """
        user = self.context.get("request").user
        return user.is_staff or user == obj.owner

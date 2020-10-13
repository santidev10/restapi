import uuid

from rest_framework.serializers import BooleanField
from rest_framework.serializers import CharField
from rest_framework.serializers import IntegerField
from rest_framework.serializers import JSONField
from rest_framework.serializers import ModelSerializer
from rest_framework.serializers import SerializerMethodField

from segment.models import CustomSegment
from segment.models import CustomSegmentFileUpload
from segment.models import CustomSegmentSourceFileUpload
from segment.models.constants import CUSTOM_SEGMENT_DEFAULT_IMAGE_URL
from segment.models.constants import SegmentListType
from segment.models.constants import SegmentTypeEnum
from segment.models.persistent.constants import S3_PERSISTENT_SEGMENT_DEFAULT_THUMBNAIL_URL
from userprofile.models import UserProfile


class FeaturedImageUrlMixin:
    """
    Returns a default image if not set
    """

    def get_featured_image_url(self, instance):
        return instance.featured_image_url or CUSTOM_SEGMENT_DEFAULT_IMAGE_URL

    def get_thumbnail_image_url(self, instance):
        """
        for backwards compatibility with frontend that expects this field
        """
        return self.get_featured_image_url(instance)


class CustomSegmentSerializer(FeaturedImageUrlMixin, ModelSerializer):
    segment_type = CharField(max_length=10)
    list_type = CharField(max_length=10)
    owner_id = CharField(max_length=50, required=False)
    statistics = JSONField(required=False)
    title = CharField(max_length=255, required=True)
    title_hash = IntegerField()
    is_vetting_complete = BooleanField(required=False)
    is_featured = BooleanField(read_only=True)
    is_regenerating = BooleanField(read_only=True)
    thumbnail_image_url = SerializerMethodField(read_only=True)
    source_name = SerializerMethodField(read_only=True)

    class Meta:
        model = CustomSegment
        fields = (
            "id",
            "audit_id",
            "created_at",
            "updated_at",
            "list_type",
            "owner_id",
            "segment_type",
            "statistics",
            "title",
            "title_hash",
            "is_vetting_complete",
            "is_featured",
            "is_regenerating",
            "thumbnail_image_url",
            "source_name",
        )

    def create(self, validated_data):
        validated_data.update({
            "uuid": uuid.uuid4()
        })
        return CustomSegment.objects.create(**validated_data)

    def validate_list_type(self, list_type):
        try:
            data = SegmentListType[list_type.upper().strip()].value
        except KeyError:
            raise ValueError("list_type must be either whitelist or blacklist.")
        return data

    def validate_segment_type(self, segment_type):
        segment_type = int(segment_type)
        if segment_type not in (0, 1):
            raise ValueError("segment_type must be either 0 or 1.")
        return segment_type

    def validate_owner(self, owner_id):
        try:
            user = UserProfile.objects.get(id=owner_id)
        except UserProfile.DoesNotExist:
            raise ValueError("User with id: {} not found.".format(owner_id))
        return user

    def validate_title(self, title):
        hashed = self.initial_data["title_hash"]
        owner_id = self.initial_data["owner_id"]
        segment_type = self.validate_segment_type(self.initial_data["segment_type"])
        segments = CustomSegment.objects.filter(owner_id=owner_id, title_hash=hashed, segment_type=segment_type)
        if any(segment.title.lower() == title.lower().strip() for segment in segments):
            raise ValueError("A {} target list with the title: {} already exists.".format(
                SegmentTypeEnum(segment_type).name.lower(), title))
        return title

    def to_representation(self, instance):
        data = super().to_representation(instance)
        data.pop("title_hash", None)
        data["pending"] = not bool(data["statistics"])
        # adding this here instead of using a SerializerMethodField to preserve to-db serialization
        data["segment_type"] = SegmentTypeEnum(instance.segment_type).name.lower()
        if not data["statistics"]:
            data["statistics"] = {
                "top_three_items": [{
                    "image_url": S3_PERSISTENT_SEGMENT_DEFAULT_THUMBNAIL_URL,
                    "id": None,
                    "title": None
                } for _ in range(3)]
            }
        try:
            # instance data should overwrite export query params as it is the most up-to-date
            export_query_params = instance.export.query.get("params", {})
            export_query_params.update(data)
            data = export_query_params
            data["download_url"] = instance.export.download_url
        except CustomSegmentFileUpload.DoesNotExist:
            data["download_url"] = None
        return data

    def get_source_name(self, obj):
        """ Get name of uploaded source file """
        try:
            name = obj.source.name
        except CustomSegmentSourceFileUpload.DoesNotExist:
            name = None
        return name


class CustomSegmentWithoutDownloadUrlSerializer(CustomSegmentSerializer):
    def to_representation(self, instance):
        """
        overrides CustomSegmentSerializer. Users without certain permissions
        shouldn't be able to see download_url
        """
        data = super().to_representation(instance)
        data.pop("download_url", None)
        return data

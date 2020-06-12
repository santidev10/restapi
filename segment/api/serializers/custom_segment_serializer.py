from rest_framework.serializers import BooleanField
from rest_framework.serializers import CharField
from rest_framework.serializers import IntegerField
from rest_framework.serializers import JSONField
from rest_framework.serializers import ModelSerializer
from rest_framework.serializers import SerializerMethodField
from segment.models import CustomSegment
from segment.models import CustomSegmentFileUpload
from segment.models.constants import CUSTOM_SEGMENT_DEFAULT_IMAGE_URL
from segment.models.persistent.constants import S3_PERSISTENT_SEGMENT_DEFAULT_THUMBNAIL_URL
from userprofile.models import UserProfile
import uuid

class FeaturedImageUrlMixin:
    """
    Returns a default image if not set
    """
    def get_featured_image_url(self, instance):
        return instance.featured_image_url or CUSTOM_SEGMENT_DEFAULT_IMAGE_URL


class CustomSegmentSerializer(FeaturedImageUrlMixin, ModelSerializer):
    segment_type = CharField(max_length=10)
    list_type = CharField(max_length=10)
    owner = CharField(max_length=50, required=False)
    statistics = JSONField(required=False)
    title = CharField(max_length=255, required=True)
    title_hash = IntegerField()
    is_vetting_complete = BooleanField(required=False)
    is_featured = BooleanField(read_only=True)
    is_regenerating = BooleanField(read_only=True)
    featured_image_url = SerializerMethodField(read_only=True)

    class Meta:
        model = CustomSegment
        fields = (
            "id",
            "audit_id",
            "created_at",
            "updated_at",
            "list_type",
            "owner",
            "segment_type",
            "statistics",
            "title",
            "title_hash",
            "is_vetting_complete",
            "is_featured",
            "is_regenerating",
            "featured_image_url",
        )

    def create(self, validated_data):
        validated_data.update({
            "uuid": uuid.uuid4()
        })
        return CustomSegment.objects.create(**validated_data)

    def validate_list_type(self, list_type):
        try:
            data = self.map_to_id(list_type.lower().strip(), item_type="list")
        except KeyError:
            raise ValueError("list_type must be either whitelist or blacklist.")
        return data

    def validate_segment_type(self, segment_type):
        segment_type = int(segment_type)
        if segment_type != 0 and segment_type != 1:
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
        owner_id = self.initial_data["owner"]
        segment_type = self.validate_segment_type(self.initial_data["segment_type"])
        segments = CustomSegment.objects.filter(owner_id=owner_id, title_hash=hashed, segment_type=segment_type)
        if any(segment.title.lower() == title.lower().strip() for segment in segments):
            raise ValueError("A {} target list with the title: {} already exists.".format(self.map_to_str(segment_type, item_type="segment"), title))
        return title

    def to_representation(self, instance):
        data = super().to_representation(instance)
        data.pop("owner")
        data.pop("title_hash")
        data["segment_type"] = self.map_to_str(data["segment_type"], item_type="segment")
        data["pending"] = False if data["statistics"] else True
        if not data["statistics"]:
            data["statistics"] = {
                "top_three_items": [{
                    "image_url": S3_PERSISTENT_SEGMENT_DEFAULT_THUMBNAIL_URL,
                    "id": None,
                    "title": None
                } for _ in range(3)]
            }
        try:
            data["download_url"] = instance.export.download_url
            data.update(instance.export.query.get("params", {}))
        except CustomSegmentFileUpload.DoesNotExist:
            data["download_url"] = None
        return data

    @staticmethod
    def map_to_str(value, item_type="segment"):
        config = {
            "segment": dict(CustomSegment.SEGMENT_TYPE_CHOICES),
            "list": dict(CustomSegment.LIST_TYPE_CHOICES)
        }
        to_str = config[item_type][int(value)]
        return to_str

    @staticmethod
    def map_to_id(value, item_type="segment"):
        config = {
            "segment": CustomSegment.segment_type_to_id,
            "list": CustomSegment.list_type_to_id
        }
        to_id = config[item_type][value]
        return to_id


class CustomSegmentWithoutDownloadUrlSerializer(CustomSegmentSerializer):
    def to_representation(self, instance):
        """
        overrides CustomSegmentSerializer. Users without certain permissions
        shouldn't be able to see download_url
        """
        data = super().to_representation(instance)
        data.pop('download_url', None)
        return data

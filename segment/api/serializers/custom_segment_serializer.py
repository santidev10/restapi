from rest_framework.serializers import CharField
from rest_framework.serializers import IntegerField
from rest_framework.serializers import JSONField
from rest_framework.serializers import ModelSerializer
from rest_framework.serializers import ValidationError
import uuid

from segment.models.persistent.constants import S3_PERSISTENT_SEGMENT_DEFAULT_THUMBNAIL_URL

from segment.models import CustomSegment
from userprofile.models import UserProfile


class CustomSegmentSerializer(ModelSerializer):
    segment_type = CharField(max_length=10)
    list_type = CharField(max_length=10)
    owner = CharField(max_length=50, required=False)
    statistics = JSONField(required=False)
    title = CharField(max_length=255, required=True)
    title_hash = IntegerField()

    class Meta:
        model = CustomSegment
        fields = (
            "id",
            "created_at",
            "updated_at",
            "list_type",
            "owner",
            "segment_type",
            "statistics",
            "title",
            "title_hash",
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
            raise ValidationError("list_type must be either whitelist or blacklist.")
        return data

    def validate_segment_type(self, segment_type):
        try:
            data = self.map_to_id(segment_type.lower().strip(), item_type="segment")
        except KeyError:
            raise ValidationError("segment_type must be either video or channel.")
        return data

    def validate_owner(self, owner_id):
        try:
            user = UserProfile.objects.get(id=owner_id)
        except UserProfile.DoesNotExist:
            raise ValidationError("User with id: {} not found.".format(owner_id))
        return user

    def validate_title(self, title):
        hashed = self.initial_data["title_hash"]
        owner_id = self.initial_data["owner"]
        segment_type = self.validate_segment_type(self.initial_data["segment_type"])
        segments = CustomSegment.objects.filter(owner_id=owner_id, title_hash=hashed, segment_type=segment_type)
        if any(segment.title.lower() == title.lower().strip() for segment in segments):
            raise ValidationError("A {} segment with the title: {} already exists.".format(self.map_to_str(segment_type, item_type="segment"), title))
        return title

    def to_representation(self, instance):
        data = super().to_representation(instance)
        data.pop("owner")
        data.pop("title_hash")
        data["segment_type"] = self.map_to_str(data["segment_type"], item_type="segment")
        data["list_type"] = self.map_to_str(data["list_type"], item_type="list")
        data["download_url"] = instance.export.download_url
        data["pending"] = True if data["statistics"].get("adw_data") is None else False
        if not data["statistics"]:
            data["statistics"] = {
                "top_three_items": [{
                    "image_url": S3_PERSISTENT_SEGMENT_DEFAULT_THUMBNAIL_URL,
                    "id": None,
                    "title": None
                } for _ in range(3)]
            }
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
            "segment": CustomSegment.segment_type_to_id ,
            "list": CustomSegment.list_type_to_id
        }
        to_id = config[item_type][value]
        return to_id
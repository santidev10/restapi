"""
Segment api serializers module
"""
from django.db.models import F
from rest_framework.serializers import CharField
from rest_framework.serializers import DictField
from rest_framework.serializers import IntegerField
from rest_framework.serializers import JSONField
from rest_framework.serializers import ListField
from rest_framework.serializers import ModelSerializer
from rest_framework.serializers import SerializerMethodField
from rest_framework.serializers import ValidationError

from segment.models import PersistentSegmentChannel
from segment.models.persistent.constants import S3_PERSISTENT_SEGMENT_DEFAULT_THUMBNAIL_URL
from segment.models import SegmentKeyword
from segment.models import CustomSegment
from segment.tasks import fill_segment_from_filters
from singledb.connector import SingleDatabaseApiConnector
from userprofile.models import UserProfile


class SegmentSerializer(ModelSerializer):
    owner = SerializerMethodField()
    shared_with = ListField(required=False)
    is_editable = SerializerMethodField()
    ids_to_add = ListField(required=False)
    ids_to_delete = ListField(required=False)
    ids_to_create = ListField(required=False)
    filters = DictField(required=False)
    pending_updates = IntegerField(read_only=True)

    title = CharField(
        max_length=255, required=True, allow_null=False, allow_blank=False)

    class Meta:
        # fixme: replace SegmentKeyword with None. It's a workaround to fix documentation generation
        model = SegmentKeyword
        fields = (
            "adw_data",
            "category",
            "created_at",
            "filters",
            "id",
            "ids_to_add",
            "ids_to_create",
            "ids_to_delete",
            "is_editable",
            "owner",
            "segment_type",
            "shared_with",
            "statistics",
            "title",
            "pending_updates",
            "updated_at",
        )

    def __init__(self, *args, **kwargs):
        """
        Extend initializing procedure
        """
        context = kwargs.get('context')
        request = context.get("request")
        fields = request.query_params.get('fields')
        if fields:
            fields = fields.split(',')
        super(SegmentSerializer, self).__init__(*args, **kwargs)
        if fields is not None:
            requested_fields = set(fields)
            pre_defined_fields = set(self.fields.keys())
            difference = pre_defined_fields - requested_fields
            for field_name in difference:
                self.fields.pop(field_name)

    def get_is_editable(self, obj):
        """
        Check segment can be edited
        """
        user = self.context.get("request").user
        return user.is_staff or user == obj.owner

    def get_owner(self, obj):
        """
        Owner first name + last name
        """
        if obj.owner:
            return obj.owner.get_full_name()
        return "Owner not found or deleted"

    def validate(self, data):
        """
        Check segment type and category
        """
        # set up related_ids
        self.ids_to_add = data.pop("ids_to_add", [])
        self.ids_to_delete = data.pop("ids_to_delete", [])
        self.ids_to_create = data.pop("ids_to_create", [])
        self.filters = data.pop("filters", None)
        segment_category = data.get("category")
        user = self.context.get("request").user
        available_categories = dict(self.Meta.model.CATEGORIES).keys()

        # create new segment
        if not self.instance:
            data['owner'] = user

            if not segment_category:
                raise ValidationError("category: value is required")

        if not user.is_staff and segment_category and segment_category != "private":
            raise ValidationError("Not valid category. Options are: private")

        if segment_category and segment_category not in available_categories:
            raise ValidationError("Not valid category. Options are: {}".format(", ".join(available_categories)))

        return data

    def save(self, **kwargs):
        segment = super(SegmentSerializer, self).save(**kwargs)
        if self.ids_to_delete or self.ids_to_add:
            segment.add_related_ids(self.ids_to_add)
            segment.delete_related_ids(self.ids_to_delete)
        if self.ids_to_create:
            sdb_connector = SingleDatabaseApiConnector()
            sdb_connector.post_channels(self.ids_to_create)
            segment.add_related_ids(self.ids_to_create)
        if self.filters is not None:
            type(segment).objects.filter(pk=segment.pk).update(pending_updates=F("pending_updates") + 1)
            fill_segment_from_filters.delay(segment.segment_type, segment.pk, self.filters)
            segment.refresh_from_db()
        if any((self.ids_to_add, self.ids_to_delete, self.ids_to_create, self.filters)):
            segment.update_statistics()
            segment.sync_recommend_channels(self.ids_to_add)
        return segment


class PersistentSegmentSerializer(ModelSerializer):
    statistics = SerializerMethodField()

    statistics_fields = (
        "subscribers",
        "likes",
        "dislikes",
        "views",
        "audited_videos",
        "items_count",
    )

    class Meta:
        # fixme: replace PersistentSegmentChannel with None. It's a workaround to fix documentation generation
        model = PersistentSegmentChannel
        fields = (
            "id",
            "title",
            "segment_type",
            "statistics",
            "thumbnail_image_url",
            "created_at",
            "updated_at",
            "category",
            "is_master"
        )

    def get_statistics(self, obj):
        details = obj.details or {}
        statistics = {field: details[field] for field in self.statistics_fields if field in details.keys()}
        return statistics


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
            "title_hash"
        )

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

"""
Segment api serializers module
"""
from rest_framework.serializers import CharField
from rest_framework.serializers import DictField
from rest_framework.serializers import ListField
from rest_framework.serializers import ModelSerializer
from rest_framework.serializers import SerializerMethodField
from rest_framework.serializers import ValidationError

from segment.models import PersistentSegmentChannel
from segment.models import SegmentKeyword
from segment.tasks import fill_segment_from_filters
from singledb.connector import SingleDatabaseApiConnector


class SegmentSerializer(ModelSerializer):
    owner = SerializerMethodField()
    shared_with = ListField(required=False)
    is_editable = SerializerMethodField()
    ids_to_add = ListField(required=False)
    ids_to_delete = ListField(required=False)
    ids_to_create = ListField(required=False)
    filters = DictField(required=False)

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
            fill_segment_from_filters.delay(segment.segment_type, segment.pk, self.filters)
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
        )

    def get_statistics(self, obj):
        details = obj.details or {}
        statistics = {field: details[field] for field in self.statistics_fields if field in details.keys()}
        return statistics

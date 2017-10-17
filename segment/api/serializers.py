from rest_framework.serializers import ListField
from rest_framework.serializers import ModelSerializer
from rest_framework.serializers import SerializerMethodField
from rest_framework.serializers import ValidationError

from segment.models import SegmentChannel
from segment.models import get_segment_model_by_type


class SegmentSerializer(ModelSerializer):
    owner = SerializerMethodField()
    is_editable = SerializerMethodField()
    ids_to_add = ListField(required=False)
    ids_to_delete = ListField(required=False)
    statistics = SerializerMethodField()

    class Meta:
        model = None
        fields = ('id',
                  'title',
                  'segment_type',
                  'category',
                  'statistics',
                  'mini_dash_data',
                  'owner',
                  'created_at',
                  'is_editable',
                  'ids_to_add',
                  'ids_to_delete')

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
        return

    def validate(self, data):
        """
        Check segment type and category
        """
        # set up related_ids
        self.ids_to_add = data.pop("ids_to_add", [])
        self.ids_to_delete = data.pop("ids_to_delete", [])
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
            segment.update_statistics(segment)
            segment.sync_recommend_channels(self.ids_to_add)
        return segment

    def get_statistics(self, instance):
        """
        Prepare segment statistics
        """
        return instance.get_statistics(**self.context)

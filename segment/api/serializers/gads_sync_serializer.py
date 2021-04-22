from rest_framework import serializers
from rest_framework.exceptions import ValidationError

from oauth.models import AdGroup
from segment.models import CustomSegment
from segment.models import SegmentAdGroupSync
from utils.views import get_object
from utils.db.functions import safe_bulk_create


class GadsSyncSerializer(serializers.Serializer):
    adgroup_ids = serializers.ListField()
    segment_id = serializers.IntegerField()

    def validate_segment_id(self, segment_id):
        segment = get_object(CustomSegment, id=segment_id, should_raise=False)
        if segment is None:
            raise ValidationError(f"Invalid CTL id: {segment_id}")
        return segment_id

    def validate_adgroup_ids(self, adgroup_ids):
        adgroups = AdGroup.objects.filter(id__in=adgroup_ids)
        remains = set(adgroups.values_list("id", flat=True)) - set(adgroup_ids)
        if remains:
            raise ValidationError(f"Invalid AdGroup ids: {remains}")
        return adgroup_ids

    def create(self, validated_data):
        to_update = []
        to_create = []
        adgroup_ids = validated_data["adgroup_ids"]
        segment_id = validated_data["segment_id"]
        exists = {
            sync.adgroup_id: sync.id for sync in SegmentAdGroupSync.objects.filter(adgroup_id__in=adgroup_ids)
        }
        for ag_id in adgroup_ids:
            sync = SegmentAdGroupSync(adgroup_id=ag_id, is_synced=False, segment_id=segment_id)
            if ag_id in exists:
                sync.id = exists[ag_id]
                container = to_update
            else:
                container = to_create
            container.append(sync)
        SegmentAdGroupSync.objects.bulk_update(to_update, fields=["is_synced", "segment"])
        safe_bulk_create(SegmentAdGroupSync, to_create, batch_size=100)
        return to_create + to_update

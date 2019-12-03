from datetime import timedelta

from django.db.models import Q
from django.utils import timezone

from segment.models import CustomSegment
from segment.tasks.generate_segment import generate_segment
from segment.models import CustomSegmentFileUpload

import time

UPDATE_THRESHOLD = 7

def update_custom_segment(segment_id):
    threshold = timezone.now() - timedelta(days=UPDATE_THRESHOLD)
    export_to_update = CustomSegmentFileUpload.objects.filter(
        (Q(updated_at__isnull=True) & Q(created_at__lte=threshold)) | Q(updated_at__lte=threshold)
    ).first()
    segment = export_to_update.segment
    results = generate_segment(segment, export_to_update.query, segment.LIST_SIZE)
    export_to_update.download_url = results["download_url"]
    segment.statistics = results["statistics"]


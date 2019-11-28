from django.utils import timezone

from segment.models import CustomSegment
from segment.tasks.generate_segment import generate_segment

import time


def generate_custom_segment(segment_id, updating=False):
    start = time.time()
    segment = CustomSegment.objects.get(id=segment_id)
    export = segment.export

    results = generate_segment(segment, export.query, segment.LIST_SIZE)
    end = time.time()
    pass
    # segment.statistics = results["statistics"]
    # export.download_url = results["download_url"]
    now = timezone.now()


    # if updating:
    #     export.updated_at = now
    # else:
    #     export.completed_at = now
    # segment.save()
    # export.save()
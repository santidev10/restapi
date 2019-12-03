from django.utils import timezone

from saas import celery_app
from segment.models import CustomSegment
from segment.tasks.generate_segment import generate_segment


@celery_app.task
def generate_custom_segment(segment_id):
    segment = CustomSegment.objects.get(id=segment_id)
    export = segment.export
    results = generate_segment(segment, export.query, segment.LIST_SIZE)
    segment.statistics = results["statistics"]
    export.download_url = results["download_url"]
    now = timezone.now()
    export.completed_at = now
    # segment.save()
    # export.save()
import logging

from saas import celery_app
from segment.utils import get_segment_model_by_type

logger = logging.getLogger(__name__)


@celery_app.task
def fill_segment_from_filters(segment_type, segment_id, filters):
    model = get_segment_model_by_type(segment_type)
    segment = model.objects.get(pk=segment_id)
    segment.add_by_filters(filters)

from segment.segment_list_generator import SegmentListGenerator
from saas import celery_app


@celery_app.task
def generate_persistent_segments():
    generation_type = 0
    SegmentListGenerator(generation_type).run()

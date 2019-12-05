from saas import celery_app
from segment.segment_list_generator import SegmentListGenerator


@celery_app.task
def generate_persistent_segments():
    SegmentListGenerator(0).run()

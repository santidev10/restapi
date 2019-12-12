import logging

from saas import celery_app
from segment.segment_list_generator import SegmentListGenerator
from utils.celery.tasks import unlock
from utils.celery.tasks import REDIS_CLIENT

LOCK_NAME = "generate_persistent_segments"
EXPIRE = 60 * 60 * 24 * 2

logger = logging.getLogger(__name__)


@celery_app.task
def generate_persistent_segments():
    is_acquired = REDIS_CLIENT.lock(LOCK_NAME, EXPIRE).acquire(blocking=False)
    if is_acquired:
        try:
            SegmentListGenerator(0).run()
        except Exception as e:
            logger.exception("Error in generate_persistent_segments task")
        finally:
            unlock(LOCK_NAME, fail_silently=True)

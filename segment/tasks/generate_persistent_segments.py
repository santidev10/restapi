import logging

from saas import celery_app
import segment.segment_list_generator as ctl_generator
from utils.celery.tasks import REDIS_CLIENT
from utils.celery.tasks import unlock

LOCK_NAME = "generate_persistent_segments"
EXPIRE = 60 * 60 * 24 * 2

logger = logging.getLogger(__name__)


@celery_app.task
def generate_persistent_segments():
    is_acquired = REDIS_CLIENT.lock(LOCK_NAME, EXPIRE).acquire(blocking=False)
    if is_acquired:
        try:
            ctl_generator.SegmentListGenerator(0).run()
        # pylint: disable=broad-except
        except Exception:
            # pylint: enable=broad-except
            logger.exception("Error in generate_persistent_segments task")
        finally:
            unlock(LOCK_NAME, fail_silently=True)

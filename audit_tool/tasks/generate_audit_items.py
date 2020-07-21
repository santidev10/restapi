from logging import getLogger

from audit_tool.segment_audit_generator import SegmentAuditGenerator
from saas import celery_app

logger = getLogger(__name__)


@celery_app.task(soft_time_limit=60 * 60 * 2)
def generate_audit_items(segment_id, data_field="video"):
    """
    Generate audit items for segment vetting process

    :param segment_id: CustomSegment id
    :param data_field: str -> video | channel
    :return:
    """
    audit_item_generator = SegmentAuditGenerator(segment_id, data_field=data_field)
    audit_item_generator.run()

from collections import defaultdict

from django.db.models import F

from audit_tool.models import AuditProcessor
from es_components.constants import Sections
from es_components.query_builder import QueryBuilder
from saas import celery_app
from segment.models.constants import SegmentTypeEnum
from segment.models import CustomSegment
from utils.utils import chunks_generator
from utils.celery.tasks import celery_lock


LOCK_NAME = "segment.update_segment_statistics"
EXPIRE = 3600 * 2


@celery_app.task(bind=True)
@celery_lock(LOCK_NAME, expire=EXPIRE, max_retries=0)
def update_segment_statistics(segment_ids=None):
    """
    Updates segment statistics values
    :return:
    """
    segment_filter = {}
    if segment_ids is not None:
        segment_filter["id__in"] = segment_ids
    else:
        segment_filter.update({
            "is_vetting_complete": False,
            "audit_id__isnull": False
        })
    to_update = CustomSegment.objects.filter(**segment_filter)
    for segment in to_update:
        try:
            audit = AuditProcessor.objects.get(id=segment.audit_id)
        except AuditProcessor.DoesNotExist:
            continue
        audit_model_ref = SegmentTypeEnum(segment.segment_type).name.lower()
        vetting_model = segment.audit_utils.vetting_model
        all_vetting_items = vetting_model.objects.filter(audit=audit)
        vetted_items = all_vetting_items.filter(processed__isnull=False)
        vetted_ids = vetted_items\
            .select_related(audit_model_ref)\
            .annotate(item_id=F(f"{audit_model_ref}__{audit_model_ref}_id"))\
            .values_list("item_id", flat=True)
        es_metrics = _get_es_counts(segment, vetted_ids)

        vetted_items_count = vetted_items.count()
        suitable_items_count = vetted_items.filter(clean=True).count()
        safe_count = es_metrics.get("safe_count", 0)
        segment.statistics.update({
            "unvetted_items_count": all_vetting_items.count() - vetted_items_count,
            "vetted_items_count": vetted_items_count,
            "safe_items_count": safe_count,
            "unsafe_items_count": vetted_items_count - safe_count,
            "suitable_items_count": suitable_items_count,
            "unsuitable_items_count": vetted_items_count - suitable_items_count,
        })
        segment.save(update_fields=["statistics"])


def _get_es_counts(segment, item_ids):
    """ Retrieve required metrics from Elasticsearch """
    results = defaultdict(int)
    es_manager = segment.es_manager
    for chunk in chunks_generator(item_ids, size=20000):
        safe_query = es_manager.ids_query(list(chunk)) \
                     & QueryBuilder().build().must_not().exists().field(f"{Sections.TASK_US_DATA}.brand_safety").get()
        safe_res = es_manager.search(safe_query, limit=0).params(track_total_hits=True).execute()
        results["safe_count"] += safe_res.hits.total.value
    return results

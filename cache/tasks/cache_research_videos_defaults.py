import logging

from cache.constants import VIDEO_AGGREGATIONS_KEY
from cache.models import CacheItem
from es_components.constants import Sections
from es_components.managers.video import VideoManager
from saas import celery_app
from saas.configs.celery import TaskExpiration
from saas.configs.celery import TaskTimeout
from utils.celery.tasks import lock
from utils.celery.tasks import unlock
from utils.es_components_api_utils import ESQuerysetAdapter
from utils.es_components_cache import set_to_cache

LOCK_NAME = "cache_research_videos_defaults"

logger = logging.getLogger(__name__)

TIMEOUT = 14400


def update_cache(obj, part, options=None, timeout=TIMEOUT):
    if part == "count":
        options = options or ((), {})
        data = obj.uncached_count()
    elif part == "get_data":
        options = options or ((0, 50), {})
        data = obj.uncached_get_data(0, 50)
    else:
        return
    set_to_cache(obj, part, options, data, timeout)


# pylint: disable=too-many-statements
@celery_app.task(expires=TaskExpiration.RESEARCH_CACHING, soft_time_limit=TaskTimeout.RESEARCH_CACHING)
def cache_research_videos_defaults():
    try:
        # pylint: disable=no-value-for-parameter
        lock(lock_name=LOCK_NAME, max_retries=60, expire=TaskExpiration.RESEARCH_CACHING)
        # pylint: enable=no-value-for-parameter
        logger.info("Starting default research videos caching.")
        default_sections = (Sections.MAIN, Sections.CHANNEL, Sections.GENERAL_DATA, Sections.BRAND_SAFETY,
                            Sections.STATS, Sections.ADS_STATS, Sections.MONETIZATION, Sections.CAPTIONS, Sections.CMS,
                            Sections.CUSTOM_CAPTIONS, Sections.CUSTOM_PROPERTIES)
        admin_sections = default_sections + (Sections.ANALYTICS,)

        fields_to_load = ["general_data", "main", "monetization", "channel", "ads_stats", "cms", "captions", "stats",
                          "brand_safety", "custom_captions", "task_us_data", "custom_properties"]

        sort = [
            {"stats.views": {"order": "desc"}},
            {"main.id": {"order": "asc"}}
        ]

        try:
            cached_aggregations_object, _ = CacheItem.objects.get_or_create(key=VIDEO_AGGREGATIONS_KEY)
            cached_aggregations = cached_aggregations_object.value
        # pylint: disable=broad-except
        except Exception:
            # pylint: enable=broad-except
            cached_aggregations = None

        # Caching for Default Sections (not Admin)
        manager = VideoManager(default_sections)
        queryset_adapter = ESQuerysetAdapter(manager, cached_aggregations=cached_aggregations)
        queryset_adapter.aggregations = []
        queryset_adapter.fields_to_load = fields_to_load
        queryset_adapter.filter_query = [manager.forced_filters()]
        queryset_adapter.percentiles = []
        queryset_adapter.sort = sort
        obj = queryset_adapter
        # Caching Count
        logger.info("Caching default research videos count.")
        part = "count"
        update_cache(obj, part)
        # Caching Get_data
        logger.info("Caching default research videos data.")
        part = "get_data"
        update_cache(obj, part)
        # Caching Count for Aggregations Query
        logger.info("Caching default research videos aggregations count.")
        obj.sort = None
        part = "count"
        update_cache(obj, part)
        # Caching Data for Aggregations Query
        logger.info("Caching default research videos aggregations data.")
        part = "get_data"
        update_cache(obj, part, options=((0, 0), {}))
        logger.info("Finished default research videos caching.")

        # Caching for Admin Sections
        admin_manager = VideoManager(admin_sections)
        admin_queryset_adapter = queryset_adapter
        admin_queryset_adapter.manager = admin_manager
        admin_fields_to_load = ["general_data", "main", "monetization", "channel", "ads_stats", "cms", "captions",
                                "stats", "brand_safety", "custom_captions", "task_us_data"]
        admin_queryset_adapter.fields_to_load = admin_fields_to_load
        obj = admin_queryset_adapter
        obj.sort = sort
        # Caching Count
        logger.info("Caching admin research videos count.")
        part = "count"
        update_cache(obj, part)
        # Caching Get_data
        logger.info("Caching admin research videos data.")
        part = "get_data"
        update_cache(obj, part)
        # Caching Count for Aggregations Query
        logger.info("Caching admin research videos aggregations count.")
        obj.sort = None
        part = "count"
        update_cache(obj, part)
        # Caching Data for Aggregations Query
        logger.info("Caching admin research videos aggregations data.")
        part = "get_data"
        update_cache(obj, part, options=((0, 0), {}))
        logger.info("Finished admin research videos caching.")
        unlock(LOCK_NAME)
    # pylint: disable=broad-except
    except Exception:
        # pylint: enable=broad-except
        pass
# pylint: enable=too-many-statements

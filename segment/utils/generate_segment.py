import logging
import os
import tempfile
from collections import defaultdict

from django.conf import settings
from elasticsearch_dsl import Q

from audit_tool.models import AuditProcessor
from segment.models import CustomSegmentSourceFileUpload
from segment.models.utils.generate_segment_utils import GenerateSegmentUtils
from segment.models.constants import ChannelConfig
from segment.models.constants import Params
from segment.models.constants import SegmentTypeEnum
from segment.models.constants import VideoConfig
from segment.utils.bulk_search import bulk_search
from segment.utils.utils import get_content_disposition
from segment.utils.utils import delete_related
from userprofile.constants import StaticPermissions
from utils.exception import retry
from utils.utils import chunks_generator


BATCH_SIZE = os.environ.get("CTL_BATCH_SIZE", 2000)
DOCUMENT_SEGMENT_ITEMS_SIZE = 100

logger = logging.getLogger(__name__)


# pylint: disable=too-many-nested-blocks,too-many-statements
@retry(count=10, delay=5, failed_callback=delete_related, failed_kwargs=dict(delete_ctl=False))
def generate_segment(segment, query_dict, size, sort=None, s3_key=None, admin_s3_key=None, options=None, add_uuid=False, with_audit=False):
    """
    Helper method to create segments
        Options determine additional filters to apply sequentially when retrieving items
        If None and for channels, first retrieves is_monetizable then non-is_monetizable items
    :param segment: CustomSegment | PersistentSegment
    :param query_dict: dict
    :param size: int -> Max row size of export
    :param sort: list -> Additional sort fields
    :param s3_key: str -> Name to use for user S3 export filename
    :param admin_s3_key: str -> Name to use for admin or vetted only S3 export filename
    :param options: list -> List of queries to sequentially apply to base query
    :param add_uuid: bool -> Add uuid to document segments section
    :param with_audit: bool -> Determines if CTL is being generated with meta audit
    :return:
    """
    generate_utils = GenerateSegmentUtils(segment)
    # file for admin or vetted only exports
    admin_filename = tempfile.mkstemp(dir=settings.TEMPDIR)[1]
    user_size = size
    # prevent user export from being overwritten by vetted export in case that segment is being vetted
    if segment.is_vetting is False:
        filename = tempfile.mkstemp(dir=settings.TEMPDIR)[1]
        if segment.segment_type in (SegmentTypeEnum.VIDEO.value, "video"):
            user_size = VideoConfig.USER_LIST_SIZE
        else:
            user_size = ChannelConfig.USER_LIST_SIZE
    context = generate_utils.default_serialization_context
    source_list = None
    # pylint: disable=broad-except
    try:
        source_list = generate_utils.get_source_list(segment)
    except (AttributeError, CustomSegmentSourceFileUpload.DoesNotExist):
        pass
    except Exception:
        logger.exception("Error trying to retrieve source list for "
                         "segment: %s, segment_type: %s", segment.title, segment.segment_type)
        raise CTLGenerateException("Unable to process source list")
    try:
        sort = sort or [segment.config.SORT_KEY]
        seen = 0
        item_ids = []
        aggregations = defaultdict(int)
        default_search_config = generate_utils.default_search_config
        # Must use bool flag to determine if we should write header instead of seen. If there is a source_list, then
        # a batch may be empty since we check set membership for the current bulk_search batch in the source list
        write_header = True
        if options is None:
            options = default_search_config["options"]
        try:
            if source_list:
                es_generator = with_source_generator(segment, source_list, query_dict, sort)
            else:
                bulk_search_kwargs = dict(
                    options=options, batch_size=BATCH_SIZE, include_cursor_exclusions=True
                )
                es_generator = bulk_search(segment.es_manager.model, query_dict, sort,
                                           default_search_config["cursor_field"], **bulk_search_kwargs)

            for batch in es_generator:
                # Clean blocklist items
                batch = generate_utils.clean_blocklist(batch, segment.segment_type)
                # Ensure that we are not adding items past limit
                batch = batch[:size - seen]
                batch_item_ids = [item.main.id for item in batch]
                item_ids.extend(batch_item_ids)
                # Get the current batch's Postgres vetting data context for serialization
                vetting = generate_utils.get_vetting_data(segment, batch_item_ids)
                context["vetting"] = vetting
                generate_utils.write_to_file(batch, admin_filename, segment.admin_export_serializer, \
                                             context, write_header=write_header is True)
                # Only write user version ctl data up to user export cap size
                if (segment.is_vetting is False) and (seen < user_size):
                    # context not required since user export only contains URL data
                    generate_utils.write_to_file(batch[:user_size - seen], filename, segment.user_export_serializer, \
                                                 {}, write_header=write_header is True)
                    # if segment is user generated, add aggregations for user version export
                    if not segment.owner.has_permission(StaticPermissions.BUILD__CTL_EXPORT_ADMIN):
                        generate_utils.add_aggregations(aggregations, batch[:user_size - seen])
                # if segment is admin generated, add aggregations for admin version export
                if segment.owner.has_permission(StaticPermissions.BUILD__CTL_EXPORT_ADMIN):
                    generate_utils.add_aggregations(aggregations, batch)
                seen += len(batch_item_ids)
                write_header = False
                if seen >= size:
                    raise MaxItemsException
        except MaxItemsException:
            pass
        if (not segment.owner.has_permission(StaticPermissions.BUILD__CTL_EXPORT_ADMIN)) and (seen > user_size):
            seen = user_size
        generate_utils.finalize_aggregations(aggregations, seen)
        if add_uuid is True:
            generate_utils.add_segment_uuid(segment, item_ids[:DOCUMENT_SEGMENT_ITEMS_SIZE])
        statistics = {
            "items_count": seen,
            **aggregations,
        }
        admin_s3_key = admin_s3_key or segment.get_admin_s3_key()
        if segment.is_vetting is False:
            s3_key = s3_key or segment.get_s3_key()
        # If with_audit is True and item_count is 0 for export, then it is unnecessary to start audit for ctl as there
        # are no further items to filter
        if with_audit is True and statistics["items_count"] > 0:
            segment.s3.export_file_to_s3(admin_filename, admin_s3_key)
            if segment.is_vetting is False:
                segment.s3.export_file_to_s3(filename, s3_key)
            # CTL export csv is finished, start audit for further filtering with inclusion_file / exclusion
            # file keywords
            generate_utils.start_audit(admin_filename)
            results = {
                "s3_key": s3_key,
                "admin_s3_key": admin_s3_key,
            }
        else:
            # Delete audit as it is no longer required since export has 0 items
            AuditProcessor.objects.filter(id=segment.params.get(Params.AuditTool.META_AUDIT_ID)).delete()
            segment.remove_meta_audit_params()
            content_disposition = get_content_disposition(segment, is_vetting=getattr(segment, "is_vetting", False))
            segment.s3.export_file_to_s3(admin_filename, admin_s3_key, extra_args={"ContentDisposition": content_disposition})
            if segment.is_vetting is False:
                segment.s3.export_file_to_s3(filename, s3_key, extra_args={"ContentDisposition": content_disposition})
            download_url = segment.s3.generate_temporary_url(admin_s3_key, time_limit=3600 * 24 * 7)
            results = {
                "statistics": statistics,
                "download_url": download_url,
                "s3_key": s3_key,
                "admin_s3_key": admin_s3_key,
            }
        return results
    except Exception:
        raise CTLGenerateException("Unable to generate export")
    finally:
        os.remove(admin_filename)
        if segment.is_vetting is False:
            os.remove(filename)
# pylint: enable=too-many-nested-blocks,too-many-statements


def with_source_generator(segment, source_ids: set, query_dict: dict, sort: list):
    """
    Retrieve documents with query dict and source_ids with batching to account for ES 10k search limit
    :param segment: CustomSegment
    :param source_ids: set
    :param query_dict: dict
    :param sort: list
    :return: iter
    """
    for chunk in chunks_generator(source_ids, BATCH_SIZE):
        with_ids = Q(query_dict) & segment.es_manager.ids_query(list(chunk))
        batch = segment.es_manager.search(with_ids, limit=BATCH_SIZE, sort=sort).execute()
        yield batch


class MaxItemsException(Exception):
    pass


class CTLGenerateException(Exception):
    message = None

    def __init__(self, message):
        super().__init__()
        self.message = message

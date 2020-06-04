from utils.utils import unique_constant_tree


@unique_constant_tree
class Name:
    SEGMENT_LIST = "segment_list"
    SEGMENT_DUPLICATE = "segment_duplicate"
    SEGMENT_SHARE = "segment_share"
    SEGMENT_CREATION_OPTIONS = "segment_creation_options"
    SEGMENT_DELETE = "segment_delete"
    SEGMENT_CREATE = "segment_create"
    SEGMENT_EXPORT = "segment_export"
    SEGMENT_SOURCE = "segment_source"

    SEGMENT_PREVIEW = "segment_preview"

    PERSISTENT_SEGMENT_LIST = "persistent_segment_list"
    PERSISTENT_SEGMENT_DETAILS = "persistent_segment_details"
    PERSISTENT_SEGMENT_EXPORT = "persistent_segment_export"
    PERSISTENT_SEGMENT_PREVIEW = "persistent_segment_preview"

    PERSISTENT_MASTER_SEGMENTS_LIST = "persistent_master_segments_list"

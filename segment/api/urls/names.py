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
    SEGMENT_OAUTH = "segment_oauth"
    SEGMENT_SYNC_GADS = "segment_sync_gads"
    SEGMENT_SYNC_DV360 = "segment_sync_dv360"
    SEGMENT_PREVIEW = "segment_preview"
    SEGMENT_GADS_SCRIPT = "segment_gads_script"

    PERSISTENT_SEGMENT_LIST = "persistent_segment_list"
    PERSISTENT_SEGMENT_DETAILS = "persistent_segment_details"
    PERSISTENT_SEGMENT_EXPORT = "persistent_segment_export"
    PERSISTENT_SEGMENT_PREVIEW = "persistent_segment_preview"

    PERSISTENT_MASTER_SEGMENTS_LIST = "persistent_master_segments_list"

    CUSTOM_SEGMENT_UPDATE = "custom_segment_update"

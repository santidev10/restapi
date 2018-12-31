from utils.utils import unique_constant_tree


@unique_constant_tree
class Name:
    SEGMENT_LIST = "segment_list"
    SEGMENT_DUPLICATE = "segment_duplicate"
    SEGMENT_SHARE = "segment_share"

    PERSISTENT_SEGMENT_LIST = "persistent_segment_list"
    PERSISTENT_SEGMENT_DETAILS = "persistent_segment_details"

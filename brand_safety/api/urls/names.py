from utils.utils import unique_constant_tree


@unique_constant_tree
class BrandSafetyPathName:
    class BadWord:
        LIST_AND_CREATE = "bad_word_list_create"
        CATEGORY_LIST = "bad_word_category_list"
        UPDATE_DELETE = "bad_word_update_delete"
        EXPORT = "bad_word_export"
        HISTORY = "bad_word_history"
        RECOVER = "bad_word_recover"

    class BrandSafety:
        GET_BRAND_SAFETY_CHANNEL = "brand_safety_channel"
        GET_BRAND_SAFETY_VIDEO = "brand_safety_video"

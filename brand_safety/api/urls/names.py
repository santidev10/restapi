from utils.utils import unique_constant_tree


@unique_constant_tree
class BrandSafetyPathName:
    class BadWord:
        LIST_AND_CREATE = "bad_word_list_create"
        CATEGORY_LIST = "bad_word_category_list"
        UPDATE_DELETE = "bad_word_update_delete"
        EXPORT = "bad_word_export"

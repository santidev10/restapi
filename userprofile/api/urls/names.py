from utils.utils import unique_constant_tree


@unique_constant_tree
class UserprofilePathName:
    AUTH = "user_auth"
    AVATAR = "avatar"
    USER_PROFILE = "user_profile"
    CREATE_USER = "user_create"
    CHANGE_PASSWORD = "change_password"
    MANAGE_PERMISSIONS = "manage_permissions"
    ROLE_LIST_CREATE = "role_list_create"
    ROLE_RETRIEVE_UPDATE = "role_retrieve_update"

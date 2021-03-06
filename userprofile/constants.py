from django.contrib.auth import get_user_model
from rest_framework import permissions

from utils.lang import ExtendedEnum

DEFAULT_DOMAIN = "viewiq"


class UserSettingsKey:
    HIDDEN_CAMPAIGN_TYPES = "hidden_campaign_types"
    VISIBLE_ACCOUNTS = "visible_accounts"

    # Deprecated, now stored on UserProfile.perms
    # Tech debt. Remove these fields after release/5.12 and 0049_migrate_permissions data migration
    # has run in production
    DASHBOARD_CAMPAIGNS_SEGMENTED = "dashboard_campaigns_segmented"
    DASHBOARD_AD_WORDS_RATES = "dashboard_ad_words_rates"
    HIDE_REMARKETING = "dashboard_remarketing_tab_is_hidden"
    DASHBOARD_COSTS_ARE_HIDDEN = "dashboard_costs_are_hidden"
    SHOW_CONVERSIONS = "show_conversions"
    VISIBLE_ALL_ACCOUNTS = "visible_all_accounts"
    GLOBAL_ACCOUNT_VISIBILITY = "global_account_visibility"

    ACTIVE_AW_SETTINGS_KEYS = {HIDDEN_CAMPAIGN_TYPES, VISIBLE_ACCOUNTS}


class UserTypeRegular(ExtendedEnum):
    AGENCY = "agency"
    BRAND = "brand"


class UserTypeCreator(ExtendedEnum):
    CREATOR = "creator"


class UserAnnualAdSpend(ExtendedEnum):
    SPEND_0_100K = "$0-$100K"
    SPEND_100K_250K = "$100K-$250K"
    SPEND_250K_AND_MORE = "$250k+"


class UserStatuses(ExtendedEnum):
    PENDING = "pending"
    REJECTED = "rejected"
    ACTIVE = "active"


class StaticPermissions:
    """
    Attribute names should follow FEATURE__PERMISSION_NAME convention
    Values should follow FEATURE.PERMISSION_NAME convention
        Descriptions of permissions are in userprofile.models.PermissionItem
    """
    ADMIN = "admin"
    ADS_ANALYZER = "ads_analyzer"
    ADS_ANALYZER__RECIPIENTS = "ads_analyzer.recipients"

    AUDIT_QUEUE__READ = "audit_queue"
    AUDIT_QUEUE__CREATE = "audit_queue.create"
    AUDIT_QUEUE__SET_PRIORITY = "audit_queue.set_priority"

    BLOCKLIST_MANAGER = "blocklist_manager"
    BLOCKLIST_MANAGER__CREATE_CHANNEL = "blocklist_manager.create_channel"
    BLOCKLIST_MANAGER__DELETE_CHANNEL = "blocklist_manager.delete_channel"
    BLOCKLIST_MANAGER__EXPORT_CHANNEL = "blocklist_manager.export_channel"
    BLOCKLIST_MANAGER__CREATE_VIDEO = "blocklist_manager.create_video"
    BLOCKLIST_MANAGER__DELETE_VIDEO = "blocklist_manager.delete_video"
    BLOCKLIST_MANAGER__EXPORT_VIDEO = "blocklist_manager.export_video"

    BSTE = "bste"
    BSTE__CREATE = "bste.create"
    BSTE__DELETE = "bste.delete"
    BSTE__EXPORT = "bste.export"

    # Custom Target Lists and Brand Suitable Featured Lists
    BUILD = "build"
    BUILD__BSTL = "build.bstl"
    BUILD__BSTL_EXPORT = "build.bstl_export"

    BUILD__CTL = "build.ctl"
    BUILD__CTL_CREATE_CHANNEL_LIST = "build.ctl_create"
    BUILD__CTL_DELETE_CHANNEL_LIST = "build.ctl_delete"
    BUILD__CTL_CREATE_VIDEO_LIST = "build.ctl_create_video_list"
    BUILD__CTL_DELETE_VIDEO_LIST = "build.ctl_delete_video_list"
    BUILD__CTL_RELEVANT_PRIMARY_CATEGORIES = "build.ctl_relevant_primary_categories"

    BUILD__CTL_FEATURE_LIST = "build.ctl_feature_list"
    BUILD__CTL_EXPORT_BASIC = "build.ctl_export_basic"
    BUILD__CTL_EXPORT_ADMIN = "build.ctl_export_admin"
    BUILD__CTL_SEE_ALL = "build.ctl_see_all"
    BUILD__CTL_VET = "build.ctl_vet"
    BUILD__CTL_VET_ENABLE = "build.ctl_vet_enable"
    BUILD__CTL_CUSTOM_VETTING_DATA = "build.ctl_custom_vetting_data"
    BUILD__CTL_REPORT_VETTING_ISSUE = "build.ctl_report_vetting_issue"
    BUILD__CTL_RESOLVE_LIMBO_STATE = "build.ctl_resolve_limbo_state"
    BUILD__CTL_FROM_CUSTOM_LIST = "build.ctl_from_custom_list"
    BUILD__CTL_PARAMS_TEMPLATE = "build.ctl_params_template"

    BUILD__CTL_VET_EXPORT = "build.ctl_vet_export"
    BUILD__CTL_VIDEO_EXCLUSION = "build.ctl_video_exclusion"

    DOMAIN_MANAGER = "domain_manager"
    DOMAIN_MANAGER__CREATE = "domain_manager.create"
    DOMAIN_MANAGER__DELETE = "domain_manager.delete"

    FORECAST_TOOL = "forecast_tool"

    MANAGED_SERVICE = "managed_service"
    MANAGED_SERVICE__EXPORT = "managed_service.export"
    MANAGED_SERVICE__PERFORMANCE_GRAPH = "managed_service.performance_graph"
    MANAGED_SERVICE__DELIVERY = "managed_service.delivery"
    MANAGED_SERVICE__CAMPAIGNS_SEGMENTED = "managed_service.campaigns_segmented"
    MANAGED_SERVICE__CONVERSIONS = "managed_service.conversions"
    MANAGED_SERVICE__VISIBLE_ALL_ACCOUNTS = "managed_service.visible_all_accounts"
    MANAGED_SERVICE__VISIBLE_DEMO_ACCOUNT = "managed_service.visible_demo_account"
    MANAGED_SERVICE__REAL_GADS_COST = "managed_service.real_gads_cost"
    MANAGED_SERVICE__GLOBAL_ACCOUNT_VISIBILITY = "managed_service.global_account_visibility"
    MANAGED_SERVICE__AUDIENCES = "managed_service.audiences"
    MANAGED_SERVICE__SERVICE_COSTS = "managed_service.service_costs"
    MANAGED_SERVICE__CHANNEL_VIDEO_TABS = "managed_service.channel_video_tabs"

    MEDIA_BUYING = "media_buying"

    PACING_REPORT = "pacing_report"

    PERFORMIQ = "performiq"
    PERFORMIQ__EXPORT = "performiq.export"

    PRICING_TOOL = "pricing_tool"

    RESEARCH = "research"
    RESEARCH__AUTH = "research.auth"
    RESEARCH__AGE_GENDER = "research.age_gender"
    RESEARCH__BRAND_SUITABILITY = "research.brand_suitability"
    RESEARCH__BRAND_SUITABILITY_HIGH_RISK = "research.brand_suitability_high_risk"
    RESEARCH__CHANNEL_VIDEO_DATA = "research.channel_video_view"
    RESEARCH__EXPORT = "research.export"
    RESEARCH__MONETIZATION = "research.monetization"
    RESEARCH__TRANSCRIPTS = "research.transcripts"
    RESEARCH__VETTING = "research.vetting"
    RESEARCH__VETTING_DATA = "research.vetting_data"

    USER_ANALYTICS = "user_analytics"
    USER_MANAGEMENT = "user_management"
    CHF_TRENDS = "chf_trends"

    DEPRECATED = {
        MANAGED_SERVICE__GLOBAL_ACCOUNT_VISIBILITY,
    }

    @staticmethod
    def has_perms(*permission_items, method=None):
        """
        Method to handle processing permissions depending on method kwarg

        :param permission_items: Variable args of permission names to be checked
            Should be constant values defined in StaticPermissions
        :param method: comma separated string of HTTP request methods that should check for permission_items
            If method is None, then all view methods will be checked with permission_items

            Example Usage:
            or_permission_classes(
                StaticPermissions.has_perms(StaticPermissions.DOMAIN_MANAGER,
                                            StaticPermissions.DOMAIN_MANAGER__READ_ALL, method="get"),
                StaticPermissions.has_perms(StaticPermissions.DOMAIN_MANAGER__CREATE, method="patch,post"),
            )

            Method call with method="get" will check StaticPermissions.DOMAIN_MANAGER and
                StaticPermissions.DOMAIN_MANAGER__READ_ALL permissions only if GET request
            Method call with method="patch,post" will check StaticPermissions.DOMAIN_MANAGER__CREATE for
                PATCH and POST requests

            If current request is using PATCH, then has_perms(method="get") will return False to allow the next
                has_perms(method="patch,post") check to handle method
        :return: bool
        """
        valid_methods = {"get", "post", "patch", "put", "delete"}

        method = set(method.split(",")) if method else valid_methods
        if method and not method.issubset(valid_methods):
            raise ValueError(f"method must be either a single string value or "
                             f"comma separated string containing only these values: {valid_methods}")

        class HasPermission(permissions.BasePermission):
            def has_permission(self, request, *_):
                if not isinstance(request.user, get_user_model()):
                    return False

                user = request.user
                request_method = request.method.lower()
                if request_method in method:
                    for perm in permission_items:
                        if user.has_permission(perm):
                            return True
                    return False

                # If current request method is not in permissions definition, return False to allow next permissions
                # definition to handle
                return False
        return HasPermission

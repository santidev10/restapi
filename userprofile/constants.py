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

    AUDIT_QUEUE = "audit_queue"
    AUDIT_QUEUE__CREATE = "audit_queue.create"
    AUDIT_QUEUE__SET_PRIORITY = "audit_queue.set_priority"

    BLOCKLIST_MANAGER = "blocklist_manager"
    BLOCKLIST_MANAGER__CREATE = "blocklist_manager.create"
    BLOCKLIST_MANAGER__DELETE = "blocklist_manager.delete"
    BLOCKLIST_MANAGER__EXPORT = "blocklist_manager.export"

    BSTE = "bste"
    BSTE__CREATE = "bste.create"
    BSTE__DELETE = "bste.delete"
    BSTE__EXPORT = "bste.export"

    BSTL = "bstl"
    BSTL__EXPORT = "bstl.export"

    CTL = "ctl"
    CTL__CREATE = "ctl.create"
    CTL__DELETE = "ctl.delete"
    CTL__FEATURE_LIST = "ctl.feature_list"
    CTL__EXPORT_BASIC = "ctl.export_basic"
    CTL__EXPORT_ADMIN = "ctl.export_admin"
    CTL__SEE_ALL = "ctl.see_all"
    CTL__VET_ENABLE = "ctl.vet_enable"
    CTL__VET = "ctl.vet"
    CTL__VET_ADMIN = "ctl.vet_admin"
    CTL__VET_EXPORT = "ctl.vet_export"

    DOMAIN_MANAGER = "domain_manager"
    DOMAIN_MANAGER__READ_ALL = "domain_manager.read_all"
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

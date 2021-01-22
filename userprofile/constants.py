from django.contrib.auth import get_user_model
from rest_framework import permissions

from utils.lang import ExtendedEnum

DEFAULT_DOMAIN = "viewiq"


class UserSettingsKey:
    DASHBOARD_CAMPAIGNS_SEGMENTED = "dashboard_campaigns_segmented"
    DASHBOARD_AD_WORDS_RATES = "dashboard_ad_words_rates"
    HIDE_REMARKETING = "dashboard_remarketing_tab_is_hidden"
    DASHBOARD_COSTS_ARE_HIDDEN = "dashboard_costs_are_hidden"
    SHOW_CONVERSIONS = "show_conversions"
    VISIBLE_ACCOUNTS = "visible_accounts"
    VISIBLE_ALL_ACCOUNTS = "visible_all_accounts"
    HIDDEN_CAMPAIGN_TYPES = "hidden_campaign_types"
    GLOBAL_ACCOUNT_VISIBILITY = "global_account_visibility"


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
    AUDIT_QUEUE__READ = "audit_queue.read"
    AUDIT_QUEUE__CREATE = "audit_queue.create"
    AUDIT_QUEUE__SET_PRIORITY = "audit_queue.set_priority"

    BLOCKLIST_MANAGER = "blocklist_manager"
    BLOCKLIST_MANAGER__READ = "blocklist_manager.read"
    BLOCKLIST_MANAGER__CREATE = "blocklist_manager.create"
    BLOCKLIST_MANAGER__DELETE = "blocklist_manager.delete"
    BLOCKLIST_MANAGER__EXPORT = "blocklist_manager.export"

    BSTE = "bste"
    BSTE__READ = "bste.read"
    BSTE__CREATE = "bste.create"
    BSTE__DELETE = "bste.delete"
    BSTE__EXPORT = "bste.export"

    BSTL = "bstl"
    BSTL__EXPORT = "bstl.export"

    CTL = "ctl"
    CTL__READ = "ctl.read"
    CTL__CREATE = "ctl.create"
    CTL__DELETE = "ctl.delete"
    CTL__FEATURE_LIST = "ctl.feature_list"
    CTL__EXPORT_BASIC = "ctl.export_basic"
    CTL__EXPORT_ADMIN = "export_admin"
    CTL__SEE_ALL = "ctl.see_all"
    CTL__VET_ENABLE = "ctl.vet_enable"
    CTL__VET = "ctl.vet"
    CTL__VET_ADMIN = "ctl.vet_admin"
    CTL__VET_EXPORT = "ctl.vet_export"

    DASHBOARD = "dashboard"

    DOMAIN_MANAGER = "domain_manager"
    DOMAIN_MANAGER__READ = "domain_manager.read"
    DOMAIN_MANAGER__CREATE = "domain_manager.create"
    DOMAIN_MANAGER__DELETE = "domain_manager.delete"

    FORECAST_TOOL = "forecast_tool"
    HEALTH_CHECK_TOOL = "health_check_tool"

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

    MEDIA_BUYING = "media_buying"

    PACING_REPORT = "pacing_report"

    PERFORMIQ = "performiq"
    PERFORMIQ__EXPORT = "performiq.export"

    PRICING_TOOL = "pricing_tool"

    RESEARCH = "research"
    RESEARCH__AGE_GENDER = "research.age_gender"
    RESEARCH__BRAND_SUITABILITY = "research.brand_suitability"
    RESEARCH__CHANNEL_DETAIL = "research.channel_detail"
    RESEARCH__EXPORT = "research.export"
    RESEARCH__MONETIZATION = "research.monetization"
    RESEARCH__TRANSCRIPTS = "research.transcripts"
    RESEARCH__VETTING = "research.vetting"
    RESEARCH__VETTING_DATA = "research.vetting_data"
    RESEARCH__VIDEO_DETAIL = "research.video_detail"

    USER_ANALYTICS = "user_analytics"
    USER_MANAGEMENT = "user_management"
    CHF_TRENDS = "chf_trends"

    def __call__(self, *permission_items):
        class HasPermission(permissions.BasePermission):
            def has_permission(self, request, *_):
                if isinstance(request.user, get_user_model()):
                    for perm in permission_items:
                        if request.user.has_permission(perm):
                            return True
                return False
        return HasPermission

    @staticmethod
    def perms():
        perm_names = [
            getattr(StaticPermissions, attr)
            for attr in dir(StaticPermissions)
            if attr[:2] != "__" and isinstance(getattr(StaticPermissions, attr), str)
        ]
        return perm_names

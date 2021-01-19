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
    AUDIT_QUEUE = "audit_queue"
    BLOCKLIST_MANAGER = "blocklist_manager"
    BSTE = "bste"

    BSTL = "bstl"

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

    MANAGED_SERVICE = "managed_service"
    PACING_REPORT = "pacing_report"

    PERFORMIQ = "performiq"
    PERFORMIQ__EXPORT = "performiq.export"

    PRICING_TOOL = "pricing_tool"

    RESEARCH = "research"
    RESEARCH__CHANNEL_DETAIL = "research.channel_detail"
    RESEARCH__EXPORT = "research.export"
    RESEARCH__VETTING = "research.vetting"
    RESEARCH__VETTING_DATA = "research.vetting_data"
    RESEARCH__VIDEO_DETAIL = "research.video_detail"
    RESEARCH__BRAND_SUITABILITY = "research.brand_suitability"

    USER_MANAGEMENT = "user_management"

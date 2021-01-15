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
    # FEATURE.PERMISSION_NAME. Descriptions of permissions are in userprofile.models.PermissionItem
    ADMIN = "admin"
    ADS_ANALYZER = "ads_analyzer"
    AUDIT_QUEUE = "audit_queue"
    BLOCKLIST_MANAGER = "blocklist_manager"
    BSTE = "bste"

    CTL = "ctl"
    CTL_READ = "ctl.read"
    CTL_CREATE = "ctl.create"
    CTL_DELETE = "ctl.delete"
    CTL_FEATURE_LIST = "ctl.feature_list"
    CTL_EXPORT_BASIC = "ctl.export_basic"
    CTL_EXPORT_ADMIN = "export_admin"
    CTL_SEE_ALL = "ctl.see_all"
    CTL_VET_ENABLE = "ctl.vet_enable"
    CTL_VET = "ctl.vet"
    CTL_VET_ADMIN = "ctl.vet_admin"
    CTL_VET_EXPORT = "ctl.vet_export"

    DOMAIN_MANAGER = "domain_manager"
    PACING_REPORT = "pacing_report"

    PERFORMIQ = "performiq"
    PERFORMIQ_EXPORT = "performiq.export"

    PRICING_TOOL = "pricing_tool"

    RESEARCH = "research"
    RESEARCH_EXPORT = "research.export"
    RESEARCH_VETTING = "research.vetting"
    RESEARCH_VETTING_DATA = "research.vetting_data"
    RESEARCH_BRAND_SUITABILITY = "research.brand_suitability"

    USER_MANAGEMENT = "user_management"

from utils.lang import ExtendedEnum


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
    DEMO_ACCOUNT_VISIBLE = "demo_account_visible"


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

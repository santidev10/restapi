from aw_reporting.models import CampaignTypeId


class AdwordsAccountSettings:
    AVAILABLE_KEYS = (
        'dashboard_campaigns_segmented',
        'demo_account_visible',
        'dashboard_ad_words_rates',
        'dashboard_remarketing_tab_is_hidden',
        'dashboard_costs_are_hidden',
        'show_conversions',
        'global_account_visibility',
        'visible_all_accounts',
    )
    CAMPAIGN_TYPES = (CampaignTypeId.DISPLAY,
                      CampaignTypeId.MULTI_CHANNEL,
                      CampaignTypeId.SEARCH,
                      CampaignTypeId.SHOPPING,
                      CampaignTypeId.VIDEO)

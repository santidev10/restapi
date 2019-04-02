from utils.utils import unique_constant_tree


@unique_constant_tree
class Name:
    TOPIC_LIST = "topic_list"
    AUDIENCE_LIST_FLAT = "flat_audience_list"

    class CreationSetup:
        CAMPAIGN = "campaign_creation_setup"
        ACCOUNT = "account_creation_setup"
        AD_DUPLICATE = "ad_creation_duplicate"
        AD_GROUP_DUPLICATE = "ad_group_creation_duplicate"
        CAMPAIGN_DUPLICATE = "campaign_creation_duplicate"

    class Dashboard:
        ACCOUNT_CREATION_BY_ACCOUNT = "account_creation_by_account"

        ACCOUNT_LIST = "dashboard_account_creation_list"
        ACCOUNT_DETAILS = "dashboard_account_details"
        ACCOUNT_OVERVIEW = "dashboard_account_overview"
        CAMPAIGNS = "dashboard_account_creation_campaigns"
        PERFORMANCE_CHART = "dashboard_performance_chart"
        PERFORMANCE_CHART_ITEMS = "dashboard_performance_chart_items"
        PERFORMANCE_EXPORT = "dashboard_performance_export"
        PERFORMANCE_EXPORT_WEEKLY_REPORT = "dashboard_export_weekly_report"

    class Analytics:
        ACCOUNT_LIST = "analytics_account_creation_list"
        ACCOUNT_DETAILS = "analytics_account_details"
        ACCOUNT_OVERVIEW = "analytics_account_overview"
        CAMPAIGNS = "analytics_account_creation_campaigns"
        PERFORMANCE_CHART = "analytics_performance_chart"
        PERFORMANCE_CHART_ITEMS = "analytics_performance_chart_items"
        PERFORMANCE_EXPORT = "analytics_performance_export"
        PERFORMANCE_EXPORT_WEEKLY_REPORT = "analytics_export_weekly_report"

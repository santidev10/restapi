from utils.utils import unique_constant_tree


@unique_constant_tree
class Name:
    class GlobalTrends:
        CHARTS = "global_trends_charts"
        DATA = "global_trends_data"
        FILTERS = "global_trends_filters"

    class Track:
        FILTERS = "track_filters"
        DATA = "track_accounts_data"
        CHART = "track_chart"

    class PacingReport:
        OPPORTUNITY_BUFFER = "pacing_report_opportunity_buffer"
        OPPORTUNITIES = "pacing_report_opportunities"
        PLACEMENTS = "pacing_report_placements"
        FLIGHTS = "pacing_report_flights"
        COLLECT = "pacing_report_collect"
        EXPORT = "pacing_report_export"
        PACING_REPORT_STATUS = "pacing_report_status"
        FLIGHTS_CAMPAIGN_ALLOCATIONS = "flights_campaign_allocations"
        FLIGHTS_CAMPAIGN_ALLOCATIONS_CHANGED = "pacing_report_flights_campaign_allocations_changed"

    class PricingTool:
        OPPORTUNITIES = "pricing_tool_opportunities"
        ESTIMATE = "pricing_tool_estimate"
        FILTERS = "pricing_tool_filters"

    class ForecastTool:
        ESTIMATE = "forecast_tool_estimate"
        FILTERS = "forecast_tool_filters"

    class Admin:
        VISIBLE_ACCOUNTS = "visible_accounts"

    class AWAccounts:
        LIST = "connect_aw_account_list"
        CONNECTION_LIST = "connect_aw_account"
        CONNECTION = "aw_account_connection"

    class WebHook:
        ACCOUNTS_LIST = "webhook_accounts_list"

    class HealthCheck:
        LIST = "health_check_tool_list"
        FILTERS = "health_check_tool_filters"

from django.conf.urls import url

from aw_reporting.api import views
from aw_reporting.api.urls.names import Name

urlpatterns = [

    # Admin
    url(r'^visible_accounts/$',
        views.VisibleAccountsApiView.as_view(),
        name=Name.Admin.VISIBLE_ACCOUNTS),
    url(r'^aw_user_settings/$',
        views.UserAWSettingsApiView.as_view(),
        name='aw_user_settings'),

    # track
    url(r'^track_filters/$',
        views.TrackFiltersListApiView.as_view(),
        name=Name.Track.FILTERS),
    url(r'^track_chart/$',
        views.TrackChartApiView.as_view(),
        name=Name.Track.CHART),
    url(r'^track_accounts_data/$',
        views.TrackAccountsDataApiView.as_view(),
        name=Name.Track.DATA),

    # connect aw account
    url(r'^connect_aw_account_list/$',
        views.ConnectAWAccountListApiView.as_view(),
        name=Name.AWAccounts.LIST),
    url(r'^connect_aw_account/$',
        views.ConnectAWAccountApiView.as_view(),
        name=Name.AWAccounts.CONNECTION_LIST),
    url(r'^connect_aw_account/(?P<email>[^/]+)/$',
        views.ConnectAWAccountApiView.as_view(),
        name=Name.AWAccounts.CONNECTION),

    # Pacing report
    url(r'^pacing_report/status/$',
        views.PacingReportStatusApiView.as_view(),
        name="pacing_report_status"),
    url(r'^pacing_report_filters/$',
        views.PacingReportFiltersApiView.as_view(),
        name="pacing_report_filters"),
    url(r'^pacing_report_flights/(?P<pk>\w+)/$',
        views.PacingReportFlightsApiView.as_view(),
        name=Name.PacingReport.FLIGHTS),
    url(r'^pacing_report_placements/(?P<pk>\w+)/$',
        views.PacingReportPlacementsApiView.as_view(),
        name=Name.PacingReport.PLACEMENTS),
    url(r'^pacing_report_opportunities/$',
        views.PacingReportOpportunitiesApiView.as_view(),
        name=Name.PacingReport.OPPORTUNITIES),
    url(r'^pacing_report_campaigns/(?P<pk>\w+)/$',
        views.PacingReportCampaignsApiView.as_view(),
        name="pacing_report_campaigns"),
    url(r'^pacing_report_update_opportunity/(?P<pk>\w+)/$',
        views.PacingReportOpportunityUpdateApiView.as_view(),
        name="pacing_report_update_opportunity"),
    url(r'^pacing_report_export/$',
        views.PacingReportExportView.as_view(),
        name=Name.PacingReport.EXPORT),
    url(r'^flights_campaign_allocations/(?P<pk>\w+)/$',
        views.PacingReportFlightsCampaignAllocationsView.as_view(),
        name=Name.PacingReport.FLIGHTS_CAMPAIGN_ALLOCATIONS),
    url(r'^flights/campaigns/budgets/updated/(?P<pk>\w+)$',
        views.PacingReportFlightsCampaignAllocationsChangedView.as_view(),
        name='pacing_report_flights_campaign_allocations_changed'),

    # AW WebHooks
    url(r'^webhook_aw/get_accounts_list/(?P<pk>\w+)/$',
        views.WebHookAWAccountsListApiView.as_view(),
        name=Name.WebHook.ACCOUNTS_LIST),
    url(r'^webhook_aw/save_settings/(?P<pk>\w+)/$',
        views.WebHookAWSaveSettingsApiView.as_view(),
        name="campaigns_setup_check_save_settings"),

    # Health check tool
    url(r'^setup_health_check_list/$',
        views.HealthCheckApiView.as_view(),
        name=Name.HealthCheck.LIST),
    url(r'^setup_health_check_filters/$',
        views.HealthCheckFiltersApiView.as_view(),
        name=Name.HealthCheck.FILTERS),

    # Pricing tool
    url(r'^pricing_tool/filters/$',
        views.PricingToolFiltersView.as_view(),
        name=Name.PricingTool.FILTERS),
    url(r'^pricing_tool/estimate/$',
        views.PricingToolEstimateView.as_view(),
        name=Name.PricingTool.ESTIMATE),
    url(r'^pricing_tool/opportunity/$',
        views.PricingToolOpportunityView.as_view(),
        name=Name.PricingTool.OPPORTUNITIES),

    # Forecast tool
    url(r'^forecast_tool/filters/$',
        views.ForecastToolFiltersApiView.as_view(),
        name=Name.ForecastTool.FILTERS),
    url(r'^forecast_tool/estimate/$',
        views.ForecastToolEstimateApiView.as_view(),
        name=Name.ForecastTool.ESTIMATE),

    # Global trends
    url(r'^global_trends/filters',
        views.GlobalTrendsFiltersApiView.as_view(),
        name=Name.GlobalTrends.FILTERS),
    url(r'^global_trends/data',
        views.GlobalTrendsDataApiView.as_view(),
        name=Name.GlobalTrends.DATA),
    url(r'^global_trends/charts',
        views.GlobalTrendsChartsApiView.as_view(),
        name=Name.GlobalTrends.CHARTS)
]

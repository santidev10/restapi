from django.conf.urls import url

from aw_reporting.api import views

urlpatterns = [
    # analyze
    url(r'^analyze_accounts_list/$',
        views.AnalyzeAccountsListApiView.as_view(),
        name="analyze_accounts_list"),
    url(r'^analyze_account_campaigns/(?P<pk>\w+)/$',
        views.AnalyzeAccountCampaignsListApiView.as_view(),
        name="analyze_account_campaigns"),
    url(r'^analyze_details/(?P<pk>\w+)/$',
        views.AnalyzeDetailsApiView.as_view(),
        name="analyze_details"),
    url(r'^analyze_chart/(?P<pk>\w+)/',
        views.AnalyzeChartApiView.as_view(),
        name="analyze_chart"),
    url(r'^analyze_chart_items/(?P<pk>\w+)/(?P<dimension>\w+)/',
        views.AnalyzeChartItemsApiView.as_view(),
        name="analyze_chart_items"),
    url(r'^analyze_export/(?P<pk>\w+)/',
        views.AnalyzeExportApiView.as_view(),
        name="analyze_export"),
    url(r'^analyze_export_weekly_report/(?P<pk>\w+)/$',
        views.AnalyzeExportWeeklyReport.as_view(),
        name="analyze_export_weekly_report"),

    # track
    url(r'^track_filters/$',
        views.TrackFiltersListApiView.as_view(),
        name="track_filters"),
    url(r'^track_chart/$',
        views.TrackChartApiView.as_view(),
        name="track_chart"),
    url(r'^track_accounts_data/$',
        views.TrackAccountsDataApiView.as_view(),
        name="track_accounts_data"),
]

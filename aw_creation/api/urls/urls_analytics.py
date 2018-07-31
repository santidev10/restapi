from django.conf.urls import url

from aw_creation.api.urls.names import Name
from aw_creation.api.views.analytics import AnalyticsAccountCreationCampaignsListApiView
from aw_creation.api.views.analytics import AnalyticsAccountCreationDetailsAPIView
from aw_creation.api.views.analytics import AnalyticsAccountCreationListApiView
from aw_creation.api.views.analytics import AnalyticsAccountCreationOverviewAPIView
from aw_creation.api.views.analytics import AnalyticsPerformanceChartApiView
from aw_creation.api.views.analytics import AnalyticsPerformanceChartItemsApiView
from aw_creation.api.views.analytics import AnalyticsPerformanceExportApiView
from aw_creation.api.views.analytics import AnalyticsPerformanceExportWeeklyReportApiView

urlpatterns = [
    url(r'^account_creation_list/$',
        AnalyticsAccountCreationListApiView.as_view(),
        name=Name.Analytics.ACCOUNT_LIST),
    url(r'^performance_account/(?P<pk>\w+)/$',
        AnalyticsAccountCreationDetailsAPIView.as_view(),
        name=Name.Analytics.ACCOUNT_DETAILS),
    url(r'^performance_account/(?P<pk>\w+)/overview/$',
        AnalyticsAccountCreationOverviewAPIView.as_view(),
        name=Name.Analytics.ACCOUNT_OVERVIEW),
    url(r'^performance_account/(?P<pk>\w+)/campaigns/$',
        AnalyticsAccountCreationCampaignsListApiView.as_view(),
        name=Name.Analytics.CAMPAIGNS),
    url(r'^performance_chart/(?P<pk>\w+)/',
        AnalyticsPerformanceChartApiView.as_view(),
        name=Name.Analytics.PERFORMANCE_CHART),
    url(r'^performance_chart_items/(?P<pk>\w+)/(?P<dimension>\w+)/',
        AnalyticsPerformanceChartItemsApiView.as_view(),
        name=Name.Analytics.PERFORMANCE_CHART_ITEMS),
    url(r'^performance_export/(?P<pk>\w+)/',
        AnalyticsPerformanceExportApiView.as_view(),
        name=Name.Analytics.PERFORMANCE_EXPORT),
    url(r'^performance_export_weekly_report/(?P<pk>\w+)/$',
        AnalyticsPerformanceExportWeeklyReportApiView.as_view(),
        name=Name.Analytics.PERFORMANCE_EXPORT_WEEKLY_REPORT),
]

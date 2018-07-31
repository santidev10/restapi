from django.conf.urls import url

from aw_creation.api.urls.names import Name
from aw_creation.api.views.dashboard import DashboardAccountCreationCampaignsListApiView
from aw_creation.api.views.dashboard import DashboardAccountCreationDetailsAPIView
from aw_creation.api.views.dashboard import DashboardAccountCreationListApiView
from aw_creation.api.views.dashboard import DashboardAccountCreationOverviewAPIView
from aw_creation.api.views.dashboard import DashboardPerformanceChartApiView

urlpatterns = [
    url(r'^account_creation_list/$',
        DashboardAccountCreationListApiView.as_view(),
        name=Name.Dashboard.ACCOUNT_LIST),
    url(r'^performance_account/(?P<pk>\w+)/$',
        DashboardAccountCreationDetailsAPIView.as_view(),
        name=Name.Dashboard.ACCOUNT_DETAILS),
    url(r'^performance_account/(?P<pk>\w+)/overview/$',
        DashboardAccountCreationOverviewAPIView.as_view(),
        name=Name.Dashboard.ACCOUNT_OVERVIEW),
    url(r'^performance_account/(?P<pk>\w+)/campaigns/$',
        DashboardAccountCreationCampaignsListApiView.as_view(),
        name=Name.Dashboard.CAMPAIGNS),
    url(r'^performance_chart/(?P<pk>\w+)/',
        DashboardPerformanceChartApiView.as_view(),
        name=Name.Dashboard.PERFORMANCE_CHART),
]

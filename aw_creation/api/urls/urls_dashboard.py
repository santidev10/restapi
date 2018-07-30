from django.conf.urls import url

from aw_creation.api.urls.names import Name
from aw_creation.api.views.dashboard import DashboardAccountCreationListApiView
from aw_creation.api.views.dashboard import DashboardAccountOverviewAPIView
from to_be_removed.performance_account_details import PerformanceAccountDetailsApiView

urlpatterns = [
    url(r'^account_creation_list/$',
        DashboardAccountCreationListApiView.as_view(),
        name=Name.Dashboard.ACCOUNT_LIST),
    url(r'^performance_account/(?P<pk>\w+)/$',
        PerformanceAccountDetailsApiView.as_view(),
        name=Name.Dashboard.ACCOUNT_DETAILS),
    url(r'^performance_account/(?P<pk>\w+)/overview$',
        DashboardAccountOverviewAPIView.as_view(),
        name=Name.Dashboard.ACCOUNT_OVERVIEW),
]

from django.conf.urls import url

from aw_creation.api.urls.names import Name
from aw_creation.api.views.analytics import AnalyticsAccountCreationListApiView
from aw_creation.api.views.analytics import AnalyticsAccountOverviewAPIView
from to_be_removed.performance_account_details import PerformanceAccountDetailsApiView

urlpatterns = [
    url(r'^account_creation_list/$',
        AnalyticsAccountCreationListApiView.as_view(),
        name=Name.Analytics.ACCOUNT_LIST),
    url(r'^performance_account/(?P<pk>\w+)/$',
        PerformanceAccountDetailsApiView.as_view(),
        name=Name.Analytics.ACCOUNT_DETAILS),
    url(r'^performance_account/(?P<pk>\w+)/overview$',
        AnalyticsAccountOverviewAPIView.as_view(),
        name=Name.Analytics.ACCOUNT_OVERVIEW),
]

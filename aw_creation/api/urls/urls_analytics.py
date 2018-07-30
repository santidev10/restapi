from django.conf.urls import url

from aw_creation.api.urls.names import Name
from aw_creation.api.views.analytics import AnalyticsAccountCreationListApiView
from aw_creation.api.views.analytics import AnalyticsAccountCreationDetailsAPIView
from aw_creation.api.views.analytics import AnalyticsAccountCreationOverviewAPIView

urlpatterns = [
    url(r'^account_creation_list/$',
        AnalyticsAccountCreationListApiView.as_view(),
        name=Name.Analytics.ACCOUNT_LIST),
    url(r'^performance_account/(?P<pk>\w+)/$',
        AnalyticsAccountCreationDetailsAPIView.as_view(),
        name=Name.Analytics.ACCOUNT_DETAILS),
    url(r'^performance_account/(?P<pk>\w+)/overview$',
        AnalyticsAccountCreationOverviewAPIView.as_view(),
        name=Name.Analytics.ACCOUNT_OVERVIEW),
]

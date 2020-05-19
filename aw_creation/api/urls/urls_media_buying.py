from django.conf.urls import url

from aw_creation.api.urls.names import Name
from aw_creation.api.views.media_buying import AccountDetailAPIView
from aw_creation.api.views.media_buying.account_targeting import AccountTargetingAPIView
from aw_creation.api.views.media_buying.account_kpi_filters import AccountKPIFiltersAPIView
from aw_creation.api.views.media_buying.account_breakout import AccountBreakoutAPIView
from aw_creation.api.views.media_buying.account_sync import AccountSyncAPIView


MediaBuying = Name.MediaBuying
urlpatterns = [
    url(r'^accounts/(?P<pk>\w+)/$',
        AccountDetailAPIView.as_view(),
        name=MediaBuying.ACCOUNT_DETAIL),

    url(r'^account/(?P<pk>\w+)/$',
        AccountDetailAPIView.as_view(),
        name=MediaBuying.ACCOUNT_DETAIL),
    url(r'^account/(?P<pk>\w+)/targeting/$',
        AccountTargetingAPIView.as_view(),
        name=MediaBuying.ACCOUNT_TARGETING),
    url(r'^account/(?P<pk>\w+)/targeting/kpi_filters/$',
        AccountKPIFiltersAPIView.as_view(),
        name=MediaBuying.ACCOUNT_KPI_FILTERS),
    url(r'^account/(?P<account_id>\w+)/sync/$',
        AccountSyncAPIView.as_view(),
        name=MediaBuying.ACCOUNT_SYNC),
    url(r'^account/(?P<pk>\w+)/breakout/$',
        AccountBreakoutAPIView.as_view(),
        name=MediaBuying.ACCOUNT_BREAKOUT),
]

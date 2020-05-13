from django.conf.urls import url

from aw_creation.api.urls.names import Name
from aw_creation.api.views.media_buying import AccountDetailAPIView
from aw_creation.api.views.media_buying.account_targeting import AccountTargetingAPIView
from aw_creation.api.views.media_buying.account_kpi_filters import AccountKPIFiltersAPIView
from aw_creation.api.views.media_buying.account_breakout import AccountBreakoutAPIView
from aw_creation.api.views.media_buying.account_sync import AccountSyncAPIView
from aw_creation.api.views.media_buying.account_campaign import AccountCampaignAPIView
from aw_creation.api.views.media_buying.account_ad_group_targeting import AccountAdGroupTargetingAPIView

MediaBuying = Name.MediaBuying
urlpatterns = [
    url(r'^account/(?P<pk>\w+)/$',
        AccountDetailAPIView.as_view(),
        name=MediaBuying.ACCOUNT_DETAIL),
    url(r'^account/(?P<pk>\w+)/targeting/$',
        AccountTargetingAPIView.as_view(),
        name=MediaBuying.ACCOUNT_TARGETING),
    url(r'^account/(?P<pk>\w+)/targeting/kpi_filters/$',
        AccountKPIFiltersAPIView.as_view(),
        name="account_kpi_filters"),
    url(r'^account/(?P<account_id>\w+)/sync/$',
        AccountSyncAPIView.as_view(),
        name="account_sync"),
    url(r'^account/(?P<pk>\w+)/breakout/$',
        AccountBreakoutAPIView.as_view(),
        name="account_breakout"),
    url(r'^campaign/(?P<pk>\w+)/$',
        AccountBreakoutAPIView.as_view(),
        name="campaign_breakout_update"),

    url(r'^account/(?P<account_id>\w+)/campaign/(?P<campaign_id>\w+)/$',
        AccountCampaignAPIView.as_view(),
        name="account_campaign"),

    url(r'^account/(?P<account_id>\w+)/ad_group/(?P<ad_group_id>\w+)/targeting/(?P<ad_group_targeting_id>\w+)',
        AccountAdGroupTargetingAPIView.as_view(),
        name="account_ad_group_targeting"),
]

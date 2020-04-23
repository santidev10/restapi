from django.conf.urls import url

from aw_creation.api.urls.names import Name
from aw_creation.api.views.media_buying import AccountDetailAPIView
from aw_creation.api.views.media_buying.account_targeting import AccountTargetingAPIView
from aw_creation.api.views.media_buying.account_kpi_filters import AccountKPIFiltersAPIView
from aw_creation.api.views.media_buying.account_breakout import AccountCampaignBreakoutAPIView
from aw_creation.api.views.media_buying.account_sync import AccountSyncAPIView

urlpatterns = [
    url(r'^account/(?P<pk>\w+)/$',
        AccountDetailAPIView.as_view(),
        name="account"),
    url(r'^account/(?P<pk>\w+)/targeting/$',
        AccountTargetingAPIView.as_view(),
        name="account_targeting"),
    url(r'^account/(?P<pk>\w+)/targeting/kpi_filters/$',
        AccountKPIFiltersAPIView.as_view(),
        name="account_kpi_filters"),

    url(r'^account/(?P<account_id>\w+)/google_ads/$',
        AccountSyncAPIView.as_view(),
        name="account_sync"),

    url(r'^account/(?P<pk>\w+)/campaign/(?P<campaign_id>\w+)/breakout/$',
        AccountCampaignBreakoutAPIView.as_view(),
        name="account_campaign_breakout"),

    url(r'^account/(?P<pk>\w+)/ad_groups/breakout/$',
        AccountCampaignBreakoutAPIView.as_view(),
        name="account_ad_group_breakout"),

    url(r'^account/(?P<pk>\w+)/ad_groups/breakout/$',
        AccountCampaignBreakoutAPIView.as_view(),
        name="account_ad_group_breakout"),
]

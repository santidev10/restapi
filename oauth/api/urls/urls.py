from django.conf.urls import url

from oauth.api import views
from .names import OAuthPathName as path_names

urlpatterns = [
    url(r"^oauth_accounts/(?P<pk>\d+)/$",
        views.OAuthAccountUpdateAPIView.as_view(),
        name=path_names.OAUTH_ACCOUNT_UPDATE),

    url(r"^oauth_accounts/$",
        views.OAuthAccountListPIView.as_view(),
        name=path_names.OAUTH_ACCOUNT_LIST),

    url(r"^gads_accounts/$",
        views.GAdsAccountListAPIView.as_view(),
        name=path_names.GADS_ACCOUNTS),

    url(r"^advertisers/$",
        views.OAuthDV360AdvertiserListAPIView.as_view(),
        name=path_names.ADVERTISER_LIST),

    url(r"^campaigns/$",
        views.OAuthCampaignListAPIView.as_view(),
        name=path_names.CAMPAIGN_LIST),

    url(r"^adgroups/$",
        views.OAuthAdGroupListAPIView.as_view(),
        name=path_names.ADGROUP_LIST),

    url(r"^insertion_orders/$",
        views.InsertionOrderListAPIView.as_view(),
        name=path_names.INSERTION_ORDER_LIST),
]

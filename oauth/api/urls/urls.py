from django.conf.urls import url

from .names import OAuthPathName as path_names
from oauth.api import views

urlpatterns = [
    # Google AdWords OAuth
    url(r"^aw_auth/$",
        views.GoogleAdsOAuthAPIView.as_view(),
        name=path_names.GADS_OAUTH),
    url(r"^aw_auth/(?P<email>[^/]+)/$",
        views.GoogleAdsOAuthAPIView.as_view(),
        name=path_names.GADS_OAUTH),
    # DV360 OAuth
    url(r"^dv360_auth/$",
        views.DV360AuthApiView.as_view(),
        name=path_names.DV360_OAUTH),

    url(r"^oauth_accounts/(?P<pk>\d+)/$",
        views.OAuthAccountUpdateAPIView.as_view(),
        name=path_names.OAUTH_ACCOUNT_UPDATE),

    url(r"^oauth_accounts/$",
        views.OAuthAccountListUpdateAPIView.as_view(),
        name=path_names.OAUTH_ACCOUNT_LIST_UPDATE),

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

    url(r"^line_items/$",
        views.LineItemListAPIView.as_view(),
        name=path_names.LINE_ITEM_LIST),
]

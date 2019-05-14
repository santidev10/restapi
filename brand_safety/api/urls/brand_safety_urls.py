from django.conf.urls import url

from brand_safety.api.urls.names import BrandSafetyPathName
from brand_safety.api.views import brand_safety as views

Names = BrandSafetyPathName.BrandSafety

urlpatterns = [
    url(
        r"^channel/(?P<pk>.+)/$",
        views.BrandSafetyChannelAPIView.as_view(),
        name=Names.GET_BRAND_SAFETY_CHANNEL,
    ),
    url(
        r"^video/(?P<pk>.+)/$",
        views.BrandSafetyVideoAPIView.as_view(),
        name=Names.GET_BRAND_SAFETY_VIDEO
    ),
]

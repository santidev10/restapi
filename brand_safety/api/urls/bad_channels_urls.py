from django.conf.urls import url

from brand_safety.api.urls.names import BrandSafetyPathName
from brand_safety.api.views import bad_channel as views

Names = BrandSafetyPathName.BadChannel

urlpatterns = [
    url(
        r'^$',
        views.BadChannelListApiView.as_view(),
        name=Names.LIST_AND_CREATE,
    ),
    url(
        r"^categories/$",
        views.BadChannelCategoryListApiView.as_view(),
        name=Names.CATEGORY_LIST
    ),
    url(
        r'^export/$',
        views.BadChannelExportApiView.as_view(),
        name=Names.EXPORT,
    ),
    url(
        r'^(?P<pk>.+)/$',
        views.BadChannelUpdateDeleteApiView.as_view(),
        name=Names.UPDATE_DELETE,
    ),
]

from django.conf.urls import url

from brand_safety.api.urls.names import BrandSafetyPathName
from brand_safety.api.views import bad_video as views

Names = BrandSafetyPathName.BadVideo

urlpatterns = [
    url(
        r'^$',
        views.BadVideoListCreateApiView.as_view(),
        name=Names.LIST_AND_CREATE,
    ),
    url(
        r"^categories/$",
        views.BadVideoCategoryListApiView.as_view(),
        name=Names.CATEGORY_LIST
    ),
    url(
        r'^(?P<pk>.+)/$',
        views.BadVideoUpdateDeleteApiView.as_view(),
        name=Names.UPDATE_DELETE,
    ),
]

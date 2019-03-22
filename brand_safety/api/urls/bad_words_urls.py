from django.conf.urls import url

from brand_safety.api.urls.names import BrandSafetyPathName
from brand_safety.api.views import bad_word as views

Names = BrandSafetyPathName.BadWord

urlpatterns = [
    url(
        r'^$',
        views.BadWordListApiView.as_view(),
        name=Names.LIST_AND_CREATE,
    ),
    url(
        r"^categories/$",
        views.BadWordCategoryListApiView.as_view(),
        name=Names.CATEGORY_LIST
    ),
    url(
        r'^export/$',
        views.BadWordExportApiView.as_view(),
        name=Names.EXPORT,
    ),
    url(
        r'^(?P<pk>.+)/$',
        views.BadWordUpdateDeleteApiView.as_view(),
        name=Names.UPDATE_DELETE,
    ),
]

from django.conf.urls import url

from brand_safety.api import views
from brand_safety.api.urls.names import BrandSafetyPathName

urlpatterns = [
    url(
        r'^bad_words/$',
        views.BadWordListApiView.as_view(),
        name=BrandSafetyPathName.BadWord.LIST_AND_CREATE,
    ),
    url(
        r'^bad_words/(?P<pk>.+)/$',
        views.BadWordUpdateDeleteApiView.as_view(),
        name=BrandSafetyPathName.BadWord.UPDATE_DELETE,
    ),
    url(
        r"^bad_words_categories/$",
        views.BadWordCategoryListApiView.as_view(),
        name=BrandSafetyPathName.BadWord.CATEGORY_LIST
    ),
]

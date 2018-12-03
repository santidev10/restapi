from django.conf.urls import url

from brand_safety.api import views
from brand_safety.api.urls.names import BrandSafetyPathName

urlpatterns = [
    url(r'^bad_words/$',
        views.BadWordListApiView.as_view(),
        name=BrandSafetyPathName.BadWord.LIST
        ),
]

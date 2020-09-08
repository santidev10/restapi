from django.conf.urls import url

from performiq.api.urls.names import PerformIQPathName
from performiq.api.views import AdWordsAuthApiView

urlpatterns = [
    # Google AdWords OAuth
    url(r"^aw_auth/$",
        AdWordsAuthApiView.as_view(),
        name=PerformIQPathName.AWAuth.CONNECTION_LIST),
    url(r"^aw_auth/(?P<email>[^/]+)/$",
        AdWordsAuthApiView.as_view(),
        name=PerformIQPathName.AWAuth.CONNECTION),
]

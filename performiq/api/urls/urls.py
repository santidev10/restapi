from django.conf.urls import url

from performiq.api.urls.names import PerformIQPathName
from performiq.api.views import AdWordsAuthApiView
from performiq.api.views import PerfromIQCampaignsAPIView

urlpatterns = [
    # Google AdWords OAuth
    url(r"^performiq/aw_auth/$",
        AdWordsAuthApiView.as_view(),
        name=PerformIQPathName.AWAuth.CONNECTION_LIST),
    url(r"^performiq/aw_auth/(?P<email>[^/]+)/$",
        AdWordsAuthApiView.as_view(),
        name=PerformIQPathName.AWAuth.CONNECTION),
    url(r"^performiq/campaigns/$", PerfromIQCampaignsAPIView.as_view(), name=PerformIQPathName.CAMPAIGNS)
]

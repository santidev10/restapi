from django.conf.urls import url

from performiq.api.urls.names import PerformIQPathName
from performiq.api.views import AdWordsAuthApiView
from performiq.api.views.dv360_auth import DV360AuthApiView
from performiq.api.views import PerformIQCampaignListCreateAPIView
from performiq.api.views import PerformIQMapCSVFieldsAPIView
from performiq.api.views import PerformIQCampaignRetrieveAPIView


urlpatterns = [
    # Google AdWords OAuth
    url(r"^performiq/aw_auth/$",
        AdWordsAuthApiView.as_view(),
        name=PerformIQPathName.AWAuth.CONNECTION_LIST),
    url(r"^performiq/aw_auth/(?P<email>[^/]+)/$",
        AdWordsAuthApiView.as_view(),
        name=PerformIQPathName.AWAuth.CONNECTION),
    # DV360 OAuth
    url(r"^performiq/dv360_auth/$",
        DV360AuthApiView.as_view(),
        name=PerformIQPathName.DV360Auth.CONNECTION_LIST),
    url(r"^performiq/campaigns/$", PerformIQCampaignListCreateAPIView.as_view(), name=PerformIQPathName.CAMPAIGNS),
    url(r"^performiq/campaigns/(?P<pk>\d+)/$", PerformIQCampaignRetrieveAPIView.as_view(),
        name=PerformIQPathName.CAMPAIGN),
    url(r"^performiq/map_csv_fields/$",
        PerformIQMapCSVFieldsAPIView.as_view(),
        name=PerformIQPathName.MAP_CSV_FIELDS),
]

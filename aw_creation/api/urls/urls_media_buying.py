from django.conf.urls import url

from aw_creation.api.urls.names import Name
from aw_creation.api.views.media_buying import AccountDetailAPIView
from aw_creation.api.views.media_buying.account_targeting import AccountTargetingAPIView
from aw_creation.api.views.media_buying.account_kpi_filters import AccountKPIFiltersAPIView

urlpatterns = [
    url(r'^account/(?P<pk>\w+)/$',
        AccountDetailAPIView.as_view(),
        name="account"),
    url(r'^account/(?P<pk>\w+)/targeting/$',
        AccountTargetingAPIView.as_view(),
        name="account_targeting"),
    url(r'^account/(?P<pk>\w+)/targeting/kpi_filters/$',
        AccountKPIFiltersAPIView.as_view(),
        name="account_kpi_filters"),
]

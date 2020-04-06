from django.conf.urls import url

from aw_creation.api.urls.names import Name
from aw_creation.api.views.media_buying import AccountDetailAPIView

urlpatterns = [
    url(r'^account/(?P<pk>\w+)/$',
        AccountDetailAPIView.as_view(),
        name="account"),
]

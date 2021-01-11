from django.conf.urls import url

from performiq.api.urls.names import  v
from performiq.api.views import AdWordsAuthApiView

urlpatterns = [
    # Google AdWords OAuth
    url(r"^ftux/list/$",
        AdWordsAuthApiView.as_view(),
        name=FTUXPathName.FTUX),
]

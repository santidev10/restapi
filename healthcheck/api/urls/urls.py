from django.conf.urls import url

from healthcheck.api.urls.names import HealthcheckPathName
from healthcheck.api.views.status import StatusApiView

urlpatterns = (
    url(r"status/", StatusApiView.as_view(), name=HealthcheckPathName.STATUS),
)

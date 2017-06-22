"""
Feedback api urls module
"""
from django.conf.urls import url

from landing.api.views import ContanctMessageSendApiView

urlpatterns = [
    url(r'^contacts/$', ContanctMessageSendApiView.as_view(), name="contacts"),
]

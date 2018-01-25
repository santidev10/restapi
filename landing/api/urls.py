"""
Feedback api urls module
"""
from django.conf.urls import url

from landing.api.views import ContanctMessageSendApiView, TopAuthChannels

urlpatterns = [
    url(r'^contacts/$', ContanctMessageSendApiView.as_view(), name="contacts"),
    url(r'^top_channels/$', TopAuthChannels.as_view(), name="top_channels"),
]

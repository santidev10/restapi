"""
Feedback api urls module
"""
from django.conf.urls import url

from landing.api.views import TopAuthChannels

urlpatterns = [
    url(r'^top_channels/$', TopAuthChannels.as_view(), name="top_channels"),
]

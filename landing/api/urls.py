"""
Feedback api urls module
"""
from django.conf.urls import url

from landing.api.names import LandingNames
from landing.api.views.top_auth_channels import TopAuthChannels

urlpatterns = [
    url(r'^top_channels/$', TopAuthChannels.as_view(), name=LandingNames.TOP_AUTH_CHANNELS),
]

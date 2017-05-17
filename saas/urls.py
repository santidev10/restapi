"""
Saas urls module
"""
from django.conf.urls import url, include

from administration.api import urls as admin_api_urls
from aw_reporting.api import urls as aw_reporting_urls
from channel.api import urls as channel_api_urls
from segment.api import urls as segment_api_urls
from userprofile.api import urls as userprofile_api_urls

urlpatterns = [
    # Userprofile api urls
    url(r'^api/v1/', include(userprofile_api_urls,
                             namespace="userprofile_api_urls")),

    # AdWords reporting api urls
    url(r'^api/v1/', include(aw_reporting_urls,
                             namespace='aw_reporting_urls')),

    # Admin api urls
    url(r'^api/v1/admin/', include(
        admin_api_urls, namespace='admin_api_urls')),

    # Channel api urls
    url(r'^api/v1/', include(channel_api_urls, namespace='channel_api_urls')),

    # Segment api urls
    url(r'^api/v1/', include(segment_api_urls, namespace='segment_api_urls')),
]

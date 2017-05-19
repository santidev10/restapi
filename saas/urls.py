"""
Saas urls module
"""
from django.conf.urls import url, include
from django.conf import settings
from django.views.generic import RedirectView
from saas.views import ApiRootView
from administration.api import urls as admin_api_urls
from aw_creation.api import urls as aw_creation_urls
from aw_reporting.api import urls as aw_reporting_urls
from keyword_tool.api import urls as keyword_tool_urls
from channel.api import urls as channel_api_urls
from video.api import urls as video_api_urls
from segment.api import urls as segment_api_urls
from userprofile.api import urls as userprofile_api_urls
from singledb.api import urls as singledb_api_urls

urlpatterns = [
    # Admin api urls
    url(r'^api/v1/admin/',
        include(admin_api_urls, namespace='admin_api_urls')),

    # AdWords creation api urls
    url(r'^api/v1/', include(aw_creation_urls,
                             namespace='aw_creation_urls')),

    # AdWords reporting api urls
    url(r'^api/v1/', include(aw_reporting_urls,
                             namespace='aw_reporting_urls')),

    # KeyWord tool api urls
    url(r'^api/v1/', include(keyword_tool_urls,
                             namespace='keyword_tool_urls')),

    # Channel api urls
    url(r'^api/v1/', include(channel_api_urls, namespace='channel_api_urls')),

    # Video api urls
    url(r'^api/v1/', include(video_api_urls, namespace='video_api_urls')),

    # Segment api urls
    url(r'^api/v1/', include(segment_api_urls, namespace='segment_api_urls')),

    # Userprofile api urls
    url(r'^api/v1/', include(userprofile_api_urls,
                             namespace="userprofile_api_urls")),

    # Singledb api urls
    url(r'^api/v1/', include(singledb_api_urls, namespace="singledb_api_urls")),
]

if settings.DEBUG:
    urlpatterns += [
        url(r'^$', RedirectView.as_view(url='api/v1/', permanent=True),
            name='redirect_root'),
        url(r'^api/v1/$', ApiRootView.as_view(), name="api_root"),
    ]

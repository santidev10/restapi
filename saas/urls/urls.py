"""
Saas urls module
"""
from django.conf.urls import url, include

from administration.api import urls as admin_api_urls
from aw_creation.api import urls as aw_creation_urls
from aw_reporting.api import urls as aw_reporting_urls
from brand_safety.api import urls as brand_safety_api_urls
from channel.api import urls as channel_api_urls
from email_reports import urls as email_reports_api_urls
from keywords.api import urls as keyword_api_urls
from landing.api import urls as landing_api_urls
from saas.urls.namespaces import Namespace
from segment.api import urls as segment_api_urls
from singledb.api import urls as singledb_api_urls
from userprofile.api import urls as userprofile_api_urls
from video.api import urls as video_api_urls

urlpatterns = [
    # Admin api urls
    url(r'^api/v1/admin/',
        include(admin_api_urls, namespace=Namespace.ADMIN)),

    # AdWords creation api urls
    url(r'^api/v1/', include(aw_creation_urls,
                             namespace=Namespace.AW_CREATION)),

    # AdWords reporting api urls
    url(r'^api/v1/', include(aw_reporting_urls,
                             namespace=Namespace.AW_REPORTING)),

    # Channel api urls
    url(r'^api/v1/', include(channel_api_urls, namespace=Namespace.CHANNEL)),

    # Video api urls
    url(r'^api/v1/', include(video_api_urls, namespace=Namespace.VIDEO)),

    # Keyword api urls
    url(r'^api/v1/', include(keyword_api_urls, namespace='keyword_api_urls')),

    # Segment api urls
    url(r'^api/v1/', include(segment_api_urls, namespace=Namespace.SEGMENT)),

    # Userprofile api urls
    url(r'^api/v1/', include(userprofile_api_urls,
                             namespace=Namespace.USER_PROFILE)),

    # Singledb api urls
    url(r'^api/v1/', include(singledb_api_urls, namespace="singledb_api_urls")),

    # landing api urls
    url(r'^api/v1/', include(landing_api_urls, namespace="landing_api_urls")),

    # Email reports
    url(r'^api/v1/', include(email_reports_api_urls,
                             namespace="email_reports_api_urls")),

    url(r'^api/v1/', include(brand_safety_api_urls, namespace=Namespace.BRAND_SAFETY)),
]

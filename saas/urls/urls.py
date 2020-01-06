"""
Saas urls module
"""
from django.conf.urls import include
from django.conf.urls import url

from administration.api import urls as admin_api_urls
from ads_analyzer.api.urls import urls as ads_analyzer_urls
from audit_tool.api import urls as audit_tool_api_urls
from aw_creation.api import urls as aw_creation_urls
from aw_reporting.api.urls.urls import urlpatterns as aw_reporting_urls
from brand_safety.api import urls as brand_safety_api_urls
from channel.api import urls as channel_api_urls
from email_reports import urls as email_reports_api_urls
from healthcheck.api.urls import urls as healthcheck_api_urls
from highlights.api import urls as highlights_api_urls
from keywords.api import urls as keyword_api_urls
from saas.urls.namespaces import Namespace
from segment.api.urls import urlpatterns as segment_v1_url_patterns
from segment.api.urls import urlpatterns_v2 as segment_v2_url_patterns
from segment.api.urls import urlpatterns_v3 as segment_v3_url_patterns
from userprofile.api import urls as userprofile_api_urls
from utils.api.urls import APP_NAME
from utils.documentation import urlpatterns as documentation_api_urls
from video.api import urls as video_api_urls

urlpatterns = [
    # Admin api urls
    url(r'^api/v1/admin/', include((admin_api_urls, APP_NAME), namespace=Namespace.ADMIN)),

    url(r'^api/v1/highlights/', include((highlights_api_urls, APP_NAME), namespace=Namespace.HIGHLIGHTS)),

    # AdWords creation api urls
    url(r'^api/v1/', include((aw_creation_urls, APP_NAME), namespace=Namespace.AW_CREATION)),

    # AdWords reporting api urls
    url(r'^api/v1/', include((aw_reporting_urls, APP_NAME), namespace=Namespace.AW_REPORTING)),

    # Channel api urls
    url(r'^api/v1/', include((channel_api_urls, APP_NAME), namespace=Namespace.CHANNEL)),

    # Video api urls
    url(r'^api/v1/', include((video_api_urls, APP_NAME), namespace=Namespace.VIDEO)),

    # Keyword api urls
    url(r'^api/v1/', include((keyword_api_urls, APP_NAME), namespace=Namespace.KEYWORD)),

    # Segment api urls
    url(r'^api/v1/', include((segment_v1_url_patterns, APP_NAME), namespace=Namespace.SEGMENT)),

    # Userprofile api urls
    url(r'^api/v1/', include((userprofile_api_urls, APP_NAME), namespace=Namespace.USER_PROFILE)),

    # Audit api urls
    url(r'^api/v1/', include((audit_tool_api_urls, APP_NAME), namespace=Namespace.AUDIT_TOOL)),

    # Email reports
    url(r'^api/v1/', include((email_reports_api_urls, APP_NAME), namespace="email_reports_api_urls")),

    url(r'^api/v1/ads_analyzer/', include((ads_analyzer_urls, APP_NAME), namespace=Namespace.ADS_ANALYZER)),

    url(r'^api/v2/', include((brand_safety_api_urls, APP_NAME), namespace=Namespace.BRAND_SAFETY)),

    url(r'^api/v2/', include((segment_v2_url_patterns, APP_NAME), namespace=Namespace.SEGMENT_V2)),

    url(r'^api/v3/', include((segment_v3_url_patterns, APP_NAME), namespace=Namespace.SEGMENT_V3)),

    url(r'^api/healthcheck/', include((healthcheck_api_urls, APP_NAME), namespace=Namespace.HEALTHCHECK)),
    url(r'^docs/', include((documentation_api_urls, APP_NAME), namespace=Namespace.DOCUMENTATION)),
]

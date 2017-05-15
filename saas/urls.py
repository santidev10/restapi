"""
Saas urls module
"""
from django.conf.urls import url, include
from administration.api import urls as admin_urls
from aw_reporting.api import urls as aw_reporting_urls
from aw_creation.api import urls as aw_creation_urls
from userprofile.api import urls as userprofile_api_urls
from singledb.api import urls as singledb_urls

urlpatterns = [
    # Userprofile api urls
    url(r'^api/v1/', include(userprofile_api_urls,
                             namespace="userprofile_api_urls")),
    # AdWords reporting api urls
    url(r'^api/v1/', include(aw_reporting_urls,
                             namespace='aw_reporting_urls')),
    # Single DB API URLs
    url(r'^api/v1/', include(singledb_urls,
                             namespace='singledb_urls')),
    # Admin api urls
    url(r'^api/v1/admin/', include(admin_urls, namespace='admin_api_urls')),
    # AdWords creation api urls
    url(r'^api/v1/', include(aw_creation_urls,
                             namespace='aw_creation_urls')),
]

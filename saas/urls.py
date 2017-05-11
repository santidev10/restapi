from django.conf.urls import url, include

from aw_reporting.api import urls as aw_reporting_urls
from singledb.api import urls as singledb_urls
from userprofile.api import urls as userprofile_api_urls


urlpatterns = [
    # Userprofile api urls
    url(r'^api/v1/', include(userprofile_api_urls,
                             namespace="userprofile_api_urls")),
    # AdWords reporting api urls
    url(r'^api/v1/', include(aw_reporting_urls,
                             namespace='aw_reporting_urls')),
    # Single DD API URLs
    url(r'^api/v1/', include(singledb_urls,
                             namespace='singledb_urls')),
]

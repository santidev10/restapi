
from django.conf.urls import url, include
from userprofile.api import urls as userprofile_api_urls
from aw_reporting.api import urls as aw_reporting_urls

urlpatterns = [
    # Userprofile api urls
    url(r'^api/v1/', include(userprofile_api_urls,
                             namespace="userprofile_api_urls")),
    # AdWords reporting api urls
    url(r'^api/v1/', include(aw_reporting_urls,
                             namespace='aw_reporting_urls')),
]

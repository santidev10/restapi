from django.conf.urls import url, include
from aw_reporting.api import urls as aw_reporting_urls

urlpatterns = [
    # AdWords reporting api urls
    url(r'^api/v1/', include(aw_reporting_urls,
                             namespace='aw_reporting_urls')),

]

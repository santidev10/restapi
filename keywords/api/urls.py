"""
Keyword api urls module
"""
from django.conf.urls import url

from keywords.api.views import KeywordListApiView
from keywords.api.views import KeywordRetrieveUpdateApiView

urlpatterns = [
    url(r'^keywords/$', KeywordListApiView.as_view(), name="keyword_list"),
    url(r'^keywords/(?P<pk>.+)/$', KeywordRetrieveUpdateApiView.as_view(), name='keywords'),
]

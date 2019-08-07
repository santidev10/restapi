"""
Keyword api urls module
"""
from django.conf.urls import url

from keywords.api import KeywordListApiView
from keywords.api.views import KeywordListExportApiView
from keywords.api.views.keyword_retrieve_update import KeywordRetrieveUpdateApiView
from .names import KeywordPathName

urlpatterns = [
    url(r'^keywords/$', KeywordListApiView.as_view(), name="keyword_list"),
    url(r'^keywords/export/$', KeywordListExportApiView.as_view(), name=KeywordPathName.KEYWORD_EXPORT),
    url(r'^keywords/(?P<pk>.+)/$', KeywordRetrieveUpdateApiView.as_view(), name='keywords'),
]

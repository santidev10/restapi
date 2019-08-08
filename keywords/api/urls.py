"""
Keyword api urls module
"""
from django.conf.urls import url

from keywords.api.views import KeywordAWStatsApiView
from keywords.api.views import KeywordListApiView
from keywords.api.views import KeywordListExportApiView
from keywords.api.views import KeywordRetrieveUpdateApiView
from .names import KeywordPathName

urlpatterns = [
    url(r'^keywords/$', KeywordListApiView.as_view(), name=KeywordPathName.KEYWORD_LIST),
    url(r'^keywords/export/$', KeywordListExportApiView.as_view(), name=KeywordPathName.KEYWORD_EXPORT),
    url(r'^keywords/(?P<pk>[^/]+)/$', KeywordRetrieveUpdateApiView.as_view(), name=KeywordPathName.KEYWORD_ITEM),
    url(r'^keywords/(?P<pk>[^/]+)/aw_stats/$', KeywordAWStatsApiView.as_view(), name=KeywordPathName.KEYWORD_AW_STATS),
]
